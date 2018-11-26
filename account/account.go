package account

import (
	"crypto/md5"
	"errors"
	"fmt"
	"github.com/iotaledger/iota.go/address"
	"github.com/iotaledger/iota.go/api"
	"github.com/iotaledger/iota.go/bundle"
	"github.com/iotaledger/iota.go/consts"
	"github.com/iotaledger/iota.go/guards"
	. "github.com/iotaledger/iota.go/trinary"
	"time"
)

var ErrEmptyMultiSendSlice = errors.New("multi send slice must be of size > 0")

type Clock interface {
	Now() time.Time
}

type realclock struct{}

func (rc *realclock) Now() time.Time {
	return time.Now()
}

type AccountEvent byte

const (
	// emitted when a transfer was broadcasted
	EventSendingTransfer AccountEvent = iota
	// emitted when a broadcasted transfer got confirmed
	EventTransferConfirmed
	// emitted when a deposit is being received
	EventReceivingDeposit
	// emitted when a deposit is confirmed
	EventReceivedDeposit
	// emitted when a zero value transaction is received
	EventReceivedMessage
	// emitted for errors of all kinds
	EventError
)

type ErrorEvent struct {
	Type  ErrorType
	Error error
}

type ErrorType byte

const (
	ErrorEventOutgoingTransfers ErrorType = iota
	ErrorEventIncomingTransfers
)

type BundleEventChannel chan bundle.Bundle

type depositgen struct {
	Addr  Hash
	Error error
}

type action byte

const (
	action_send action = iota
	action_multi_send
	action_new_deposit_address
	action_current_balance
	action_is_new
	action_trigger_transfer_polling
	action_shutdown
)

type actionrequest struct {
	Action  action
	Request interface{}
}

type Recipients []Recipient

func (recps Recipients) Sum() uint64 {
	var sum uint64
	for _, target := range recps {
		sum += target.Value
	}
	return sum
}

func (recps Recipients) AsTransfers() bundle.Transfers {
	transfers := make(bundle.Transfers, len(recps))
	for i, recipient := range recps {
		transfers[i] = recipient
	}
	return transfers
}

// Recipient is a bundle.Transfer but with a nicer name.
type Recipient = bundle.Transfer

type actionresponse struct {
	item interface{}
	err  error
}

type InputSelectionStrategyFunc func(indices []uint64) []uint64

type AccountsOpts struct {
	MWM                  uint64
	Depth                uint64
	TransferPollInterval uint64
	SecurityLevel        consts.SecurityLevel
	Clock                Clock
	ReceiveEventFilter   ReceiveEventFilter
}

func defaultAccountOpts(opts ...*AccountsOpts) *AccountsOpts {
	if len(opts) == 0 {
		return &AccountsOpts{
			MWM: 14, Depth: 3, SecurityLevel: consts.SecurityLevelMedium,
			TransferPollInterval: 30, Clock: &realclock{},
			ReceiveEventFilter: NewPerTailReceiveEventFilter(),
		}
	}
	defaultValue := func(val uint64, should uint64) uint64 {
		if val == 0 {
			return should
		}
		return val
	}
	opt := opts[0]
	if opt.SecurityLevel == 0 {
		opt.SecurityLevel = consts.SecurityLevelMedium
	}
	opt.Depth = defaultValue(opt.Depth, 3)
	opt.MWM = defaultValue(opt.MWM, 14)
	opt.TransferPollInterval = defaultValue(opt.TransferPollInterval, 10)
	if opt.Clock == nil {
		opt.Clock = &realclock{}
	}
	if opt.ReceiveEventFilter == nil {
		opt.ReceiveEventFilter = NewPerTailReceiveEventFilter()
	}
	return opt
}

func NewAccount(seed Trytes, storage Store, api *api.API, opts ...*AccountsOpts) (*Account, error) {
	opt := defaultAccountOpts(opts...)
	acc := &Account{
		id:              fmt.Sprintf("%x", md5.Sum([]byte(seed))),
		seed:            seed,
		request:         make(chan actionrequest),
		sendBackChan:    make(chan actionresponse),
		nextDepositAddr: make(chan depositgen),
		exitDepositAddr: make(chan struct{}),
		api:             api,
		storage:         storage,
		listeners: map[AccountEvent][]BundleEventChannel{
			EventSendingTransfer:   {},
			EventTransferConfirmed: {},
			EventReceivingDeposit:  {},
			EventReceivedDeposit:   {},
			EventReceivedMessage:   {},
		},
		mwm:                  opt.MWM,
		depth:                opt.Depth,
		secLvl:               opt.SecurityLevel,
		transferPollInterval: opt.TransferPollInterval,
		clock:                opt.Clock,
		receiveEventFilter:   opt.ReceiveEventFilter,
	}
	if err := acc.runEventLoop(); err != nil {
		return nil, err
	}
	return acc, nil
}

type Account struct {
	errors chan ErrorEvent

	id   string
	seed Trytes

	// internal event loop
	request         chan actionrequest
	sendBackChan    chan actionresponse
	nextDepositAddr chan depositgen
	exitDepositAddr chan struct{}

	// misc
	api     *api.API
	storage Store
	clock   Clock

	// customization
	inputselection       InputSelectionStrategyFunc
	mwm                  uint64
	depth                uint64
	secLvl               consts.SecurityLevel
	transferPollInterval uint64

	// event
	eventsEnabled      bool
	receiveEventFilter ReceiveEventFilter
	listeners          map[AccountEvent][]BundleEventChannel
}

// Send sends the specified amount to the recipient address.
func (a *Account) Send(recipient Recipient) (bundle.Bundle, error) {
	if !guards.IsTrytesOfExactLength(recipient.Address, consts.HashTrytesSize+consts.AddressChecksumTrytesSize) {
		return nil, consts.ErrInvalidAddress
	}
	a.request <- actionrequest{Action: action_send, Request: recipient}
	payload := <-a.sendBackChan
	if payload.err != nil {
		return nil, payload.err
	}
	return payload.item.(bundle.Bundle), nil
}

// Send sends the specified amounts to the recipient addresses.
func (a *Account) SendMulti(recipients Recipients) (bundle.Bundle, error) {
	if recipients == nil || len(recipients) == 0 {
		return nil, ErrEmptyMultiSendSlice
	}
	for _, target := range recipients {
		if !guards.IsTrytesOfExactLength(target.Address, consts.HashTrytesSize+consts.AddressChecksumTrytesSize) {
			return nil, consts.ErrInvalidAddress
		}
	}

	a.request <- actionrequest{Action: action_multi_send, Request: recipients}
	payload := <-a.sendBackChan
	if payload.err != nil {
		return nil, payload.err
	}
	return payload.item.(bundle.Bundle), nil
}

// NewDepositAddress generates a new deposit address.
func (a *Account) NewDepositAddress() (Hash, error) {
	a.request <- actionrequest{Action: action_new_deposit_address}
	payload := <-a.sendBackChan
	if payload.err != nil {
		return "", payload.err
	}
	return payload.item.(Hash), nil
}

// Balance gets the current balance.
func (a *Account) Balance() (uint64, error) {
	a.request <- actionrequest{Action: action_current_balance}
	payload := <-a.sendBackChan
	if payload.err != nil {
		return 0, payload.err
	}
	return payload.item.(uint64), nil
}

// IsNew checks whether the account is new.
func (a *Account) IsNew() (bool, error) {
	a.request <- actionrequest{Action: action_is_new}
	payload := <-a.sendBackChan
	if payload.err != nil {
		return false, payload.err
	}
	return payload.item.(bool), nil
}

// Shutdown cleanly shutdowns the account and releases its goroutines.
func (a *Account) Shutdown() error {
	a.request <- actionrequest{Action: action_shutdown}
	return (<-a.sendBackChan).err
}

// Errors returns a channel from which errors can be received from internal processes (in-/out going transfer polling, promotion/reattachment etc.).
func (a *Account) Errors() <-chan ErrorEvent {
	return a.errors
}

// TriggerTransferPolling triggets a transfer polling.
func (a *Account) TriggerTransferPolling() {
	a.request <- actionrequest{Action: action_trigger_transfer_polling}
}

// RegisterEventHandler registers a new event handler.
func (a *Account) RegisterEventHandler(event AccountEvent, channel BundleEventChannel) int {
	eventListeners, ok := a.listeners[event]
	if ! ok {
		return -1
	}
	eventListeners = append(eventListeners, channel)
	a.listeners[event] = eventListeners
	return len(eventListeners) - 1
}

func (a *Account) sendError(err error) {
	a.sendBackChan <- actionresponse{err: err}
}

func (a *Account) runEventLoop() error {
	_, err := a.storage.LoadAccount(a.id)
	if err != nil {
		return err
	}

	// warm up the receive event filter by polling once before starting the event loop
	a.checkIncomingTransfers()
	a.eventsEnabled = true

	// start deposit address generator
	go a.depositAddressGenerator()

	// TODO: when there are pending transfers in the database but not a single tail transaction
	// it means that the sending the transfer or storing the origin bundle tail hash failed.
	// thereby before the account event loop starts, the missing tail hash should be added.

	go func() {
		for {
			select {
			case req := <-a.request:
				switch req.Action {
				case action_send:
					a.send(Recipients{req.Request.(Recipient)})
				case action_multi_send:
					a.send(req.Request.(Recipients))
				case action_new_deposit_address:
					payload := <-a.nextDepositAddr
					a.sendBackChan <- actionresponse{item: payload.Addr, err: payload.Error}
				case action_current_balance:
					balance, err := a.balance()
					a.sendBackChan <- actionresponse{item: balance, err: err}
				case action_trigger_transfer_polling:
					a.pollTransfers()
				case action_is_new:
					state, err := a.storage.LoadAccount(a.id)
					a.sendBackChan <- actionresponse{item: state.IsNew(), err: err}
				case action_shutdown:
					// release deposit address generator
					close(a.exitDepositAddr)
					a.sendBackChan <- actionresponse{}
					// exit event loop
					return
				}
			case <-time.After(time.Duration(a.transferPollInterval) * time.Second):
				a.pollTransfers()
			}
		}
	}()
	return nil
}

func (a *Account) depositAddressGenerator() {
	for {
		state, err := a.storage.LoadAccount(a.id)
		if err != nil {
			a.nextDepositAddr <- depositgen{Error: err}
			continue
		}
		nextIndex := state.lastKeyIndex + 1
		depositAddr, err := address.GenerateAddress(a.seed, nextIndex, a.secLvl, true)
		if err != nil {
			a.nextDepositAddr <- depositgen{Error: err}
			continue
		}
		if err := a.storage.MarkDepositAddresses(a.id, nextIndex); err != nil {
			a.nextDepositAddr <- depositgen{Error: err}
			continue
		}
		select {
		case a.nextDepositAddr <- depositgen{Addr: depositAddr}:
		case <-a.exitDepositAddr:
			return
		}
	}
}

func (a *Account) emitEvent(payload interface{}, event AccountEvent) {
	// used during startup to flush events
	if !a.eventsEnabled {
		return
	}

	eventListeners := a.listeners[event]

	switch event {
	case EventSendingTransfer:
		fallthrough
	case EventReceivingDeposit:
		fallthrough
	case EventTransferConfirmed:
		fallthrough
	case EventReceivedDeposit:
		fallthrough
	case EventReceivedMessage:
		for _, listener := range eventListeners {
			select {
			case listener <- payload.(bundle.Bundle):
			default:
			}
		}
	case EventError:
		select {
		case a.errors <- payload.(ErrorEvent):
		default:
		}
	}
}

func (a *Account) send(targets Recipients) {
	var inputs []api.Input
	var remainderAddress *Hash
	var sum uint64
	var err error

	transferSum := targets.Sum()
	if transferSum > 0 {
		sum, inputs, err = a.selectInputs(transferSum)
		if err != nil {
			a.sendError(err)
			return
		}
		// generate a remainder address for the transfer
		// by adding it to the store
		if sum > transferSum {
			payload := <-a.nextDepositAddr
			if payload.Error != nil {
				a.sendError(payload.Error)
				return
			}
			remainderAddress = &payload.Addr
		}

		// TODO: maybe check for target address spent state?
	}

	transfers := targets.AsTransfers()

	ts := uint64(a.clock.Now().UnixNano() / int64(time.Second))
	opts := api.PrepareTransfersOptions{
		Inputs:           inputs,
		RemainderAddress: remainderAddress,
		Security:         a.secLvl,
		Timestamp:        &ts,
	}

	bundleTrytes, err := a.api.PrepareTransfers(a.seed, transfers, opts)
	if err != nil {
		a.sendError(err)
		return
	}

	indices := make([]uint64, len(inputs))
	for i, input := range inputs {
		indices[i] = input.KeyIndex
	}

	// add the new transfer to the db
	if err := a.storage.AddPendingTransfer(a.id, bundleTrytes, indices...); err != nil {
		a.sendError(err)
		return
	}

	bndl, err := a.api.SendTrytes(bundleTrytes, a.depth, a.mwm)
	if err != nil {
		a.sendError(err)
		return
	}

	if err := a.storage.AddTailHash(a.id, bndl[0].Bundle, bndl[0].Hash); err != nil {
		a.sendError(err)
		return
	}

	a.emitEvent(bndl, EventSendingTransfer)
	a.sendBackChan <- actionresponse{item: bndl}
}

// selects addresses as inputs which are deposit addresses,
// have no incoming transfers and have funds.
func (a *Account) selectInputs(transferValue uint64) (uint64, []api.Input, error) {
	state, err := a.storage.LoadAccount(a.id)
	if err != nil {
		return 0, nil, err
	}
	addrsToQuery := Hashes{}
	inputs := []api.Input{}
	for _, index := range state.DepositAddresses() {
		has, err := a.hasIncomingTransfer(index)
		if err != nil {
			return 0, nil, err
		}
		if has {
			continue
		}
		addr, err := address.GenerateAddress(a.seed, index, a.secLvl)
		if err != nil {
			return 0, nil, err
		}
		addrsToQuery = append(addrsToQuery, addr)
		input := api.Input{
			Address:  addr,
			KeyIndex: index,
			Security: a.secLvl,
			Balance:  0,
		}
		inputs = append(inputs, input)
	}

	balances, err := a.api.GetBalances(addrsToQuery, 100)
	if err != nil {
		return 0, nil, err
	}

	var sum uint64
	// inputs without zero balance
	filteredInputs := []api.Input{}
	for i, balance := range balances.Balances {
		if balance == 0 {
			continue
		}
		sum += balance
		inputs[i].Balance = balance
		filteredInputs = append(filteredInputs, inputs[i])
		if sum > transferValue {
			break
		}
	}

	if sum < transferValue {
		return 0, nil, consts.ErrInsufficientBalance
	}

	return sum, filteredInputs, nil
}

func (a *Account) balance() (uint64, error) {
	state, err := a.storage.LoadAccount(a.id)
	if err != nil {
		return 0, err
	}
	if state.IsNew() {
		return 0, nil
	}
	addresses := make(Hashes, len(state.DepositAddresses()))
	for i, index := range state.DepositAddresses() {
		addr, err := address.GenerateAddress(a.seed, index, a.secLvl, true)
		if err != nil {
			return 0, err
		}
		addresses[i] = addr
	}
	balances, err := a.api.GetBalances(addresses, 100)
	if err != nil {
		return 0, err
	}
	var total uint64
	for _, balance := range balances.Balances {
		total += balance
	}
	return total, nil
}

func (a *Account) hasIncomingTransfer(index uint64) (bool, error) {
	addr, err := address.GenerateAddress(a.seed, index, a.secLvl, true)
	if err != nil {
		return false, err
	}

	bundles, err := a.api.GetBundlesFromAddresses(Hashes{addr}, true)
	if err != nil {
		return false, err
	}

	for _, bndl := range bundles {
		// if the bundle is invalid don't consider it at all as an incoming transfer
		if err := bundle.ValidBundle(bndl); err != nil {
			continue
		}
		// if the bundle is not a value transfer don't consider it either
		valueTransfer := false
		for i := range bndl {
			tx := &bndl[i]
			if tx.Value < 0 || tx.Value > 0 {
				valueTransfer = true
				break
			}
		}
		if !valueTransfer {
			continue
		}
		if !*bndl[0].Persistence {
			return true, nil
		}
	}
	return false, nil
}

func (a *Account) pollTransfers() {
	a.checkOutgoingTransfers()
	a.checkIncomingTransfers()
}

func (a *Account) checkOutgoingTransfers() {
	state, _ := a.storage.LoadAccount(a.id)
	for _, pendingTransfer := range state.PendingTransfers {
		if len(pendingTransfer.Tails) == 0 {
			continue
		}
		states, err := a.api.GetLatestInclusion(pendingTransfer.Tails)
		if err != nil {
			a.emitEvent(ErrorEvent{Error: err, Type: ErrorEventOutgoingTransfers}, EventError)
			return
		}
		// if any state is true we can remove the transfer as it got confirmed
		for i, state := range states {
			if state {
				bndl, err := a.api.GetBundle(pendingTransfer.Tails[i])
				if err != nil {
					a.emitEvent(ErrorEvent{Error: err, Type: ErrorEventOutgoingTransfers}, EventError)
					return
				}
				a.emitEvent(bndl, EventTransferConfirmed)
				if err := a.storage.RemovePendingTransfer(a.id, bndl[0].Bundle); err != nil {
					a.emitEvent(ErrorEvent{Error: err, Type: ErrorEventOutgoingTransfers}, EventError)
					return
				}
				break
			}
		}
	}
}

func (a *Account) checkIncomingTransfers() {
	state, _ := a.storage.LoadAccount(a.id)
	depAddrsIndices := state.DepositAddresses()
	depsAddrs := make(Hashes, len(depAddrsIndices))
	if len(depsAddrs) == 0 {
		return
	}

	spentAddrsIndices := state.SpentAddresses()
	spentAddrs := make(Hashes, len(spentAddrsIndices))
	for i, index := range depAddrsIndices {
		addr, err := address.GenerateAddress(a.seed, index, a.secLvl, true)
		if err != nil {
			panic(err)
		}
		depsAddrs[i] = addr
	}

	for i, index := range spentAddrsIndices {
		addr, err := address.GenerateAddress(a.seed, index, a.secLvl, true)
		if err != nil {
			panic(err)
		}
		spentAddrs[i] = addr
	}

	// get all bundles which operated on the current deposit depsAddrs
	bndls, err := a.api.GetBundlesFromAddresses(depsAddrs, true)
	if err != nil {
		a.emitEvent(ErrorEvent{Error: err, Type: ErrorEventIncomingTransfers}, EventError)
		return
	}

	// create the events to emit in the event system
	for _, event := range a.receiveEventFilter.Filter(bndls, depsAddrs, spentAddrs) {
		a.emitEvent(event.Bundle, event.Event)
	}
}

// ReceiveEventFilter filters and creates events given the incoming bundles, deposit and spent addresses.
type ReceiveEventFilter interface {
	Filter(bndls bundle.Bundles, depAddrs Hashes, spentAddrs Hashes) []ReceiveEventTuple
}

type ReceiveEventTuple struct {
	Event  AccountEvent
	Bundle bundle.Bundle
}

func NewPerTailReceiveEventFilter() *PerTailFilter {
	return &PerTailFilter{
		receivedFilter:  map[string]struct{}{},
		receivingFilter: map[string]struct{}{},
	}
}

// PerTailFilter filters receiving/received bundles by the bundle's tail transaction hash.
type PerTailFilter struct {
	receivingFilter map[string]struct{}
	receivedFilter  map[string]struct{}
}

func (ptf *PerTailFilter) Filter(bndls bundle.Bundles, depAddrs Hashes, spentAddrs Hashes) []ReceiveEventTuple {
	events := []ReceiveEventTuple{}

	// filter out bundles where the addresses are non deposits
	// or an input address is an own spent address
	receivingBundles := make(map[string]bundle.Bundle)
	receivedBundles := make(map[string]bundle.Bundle)

	for _, bndl := range bndls {
		if err := bundle.ValidBundle(bndl); err != nil {
			continue
		}
		isSpend := false
		isOwnSpend := false
	outOwnSpent:
		// filter value transfers where the deposit address is the remainder address
		for _, spentAddr := range spentAddrs {
			for _, txInBundle := range bndl {
				if txInBundle.Address == spentAddr && txInBundle.Value < 0 {
					isOwnSpend = true
					break outOwnSpent
				}
			}
		}
	outSpend:
		for _, txInBundle := range bndl {
			// filter value transfers where a deposit address is an input
			for _, depAddr := range depAddrs {
				if txInBundle.Value < 0 && txInBundle.Address == depAddr {
					isSpend = true
					break outSpend
				}
			}
		}
		if isOwnSpend || isSpend {
			continue
		}
		tailTx := bundle.TailTransactionHash(bndl)
		if *bndl[0].Persistence {
			receivedBundles[tailTx] = bndl
		} else {
			receivingBundles[tailTx] = bndl
		}
	}

	isValueTransfer := func(bndl bundle.Bundle) bool {
		isValue := false
		for _, tx := range bndl {
			if tx.Value > 0 || tx.Value < 0 {
				isValue = true
				break
			}
		}
		return isValue
	}

	// filter out bundles for which a previous event was emitted
	// and emit new events for the new bundles
	for tailTx, bndl := range receivingBundles {
		if _, has := ptf.receivingFilter[tailTx]; has {
			delete(receivingBundles, tailTx)
			continue
		}
		ptf.receivingFilter[tailTx] = struct{}{}
		// determine whether the bundle is a value transfer.
		// it isn't checked whether the value is deposited to a deposit address
		if isValueTransfer(bndl) {
			events = append(events, ReceiveEventTuple{EventReceivingDeposit, bndl})
			continue
		}
		events = append(events, ReceiveEventTuple{EventReceivedMessage, bndl})
	}

	for tailTx, bndl := range receivedBundles {
		if _, has := ptf.receivedFilter[tailTx]; has {
			delete(receivedBundles, tailTx)
			continue
		}
		ptf.receivedFilter[tailTx] = struct{}{}
		if isValueTransfer(bndl) {
			events = append(events, ReceiveEventTuple{EventReceivedDeposit, bndl})
			continue
		}
		events = append(events, ReceiveEventTuple{EventReceivedMessage, bndl})
	}

	return events
}
