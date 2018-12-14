package account

import (
	"crypto/md5"
	"fmt"
	"github.com/iotaledger/iota.go/address"
	"github.com/iotaledger/iota.go/api"
	"github.com/iotaledger/iota.go/bundle"
	"github.com/iotaledger/iota.go/consts"
	"github.com/iotaledger/iota.go/guards"
	"github.com/iotaledger/iota.go/transaction"
	. "github.com/iotaledger/iota.go/trinary"
	"github.com/pkg/errors"
	"strings"
	"time"
)

var ErrEmptyMultiSendSlice = errors.New("multi send slice must be of size > 0")

type ErrMarkDepositAddr struct {
	actualError error
}

func (err ErrMarkDepositAddr) Error() string {
	return err.Error()
}

type ErrAccountPanic struct {
	internalError error
}

func (err ErrAccountPanic) Error() string {
	return fmt.Sprintf("severe account error (panic): %s", err.internalError.Error())
}

var ErrTimeoutNotSpecified = errors.New("deposit requests must have a timeout")
var ErrTimeoutTooLow = errors.New("deposit requests must at least have a timeout of >2 minutes")

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
	// emitted when a promotion occurred
	EventPromotion
	// emitted when a reattachment occurred
	EventReattachment
	// emitted for errors of all kinds
	EventError
)

type PromotionReattachmentEvent struct {
	OriginTailTxHash       Hash
	BundleHash             Hash
	PromotionTailTxHash    Hash
	ReattachmentTailTxHash Hash
}

type ErrorEvent struct {
	Type  ErrorType
	Error error
}

type ErrorType byte

const (
	ErrorEventOutgoingTransfers ErrorType = iota
	ErrorEventIncomingTransfers
	ErrorPromoteTransfer
	ErrorReattachTransfer
	ErrorInternal
)

type EventChannel interface {
	Channel() chan interface{}
}

type eventchannel struct {
	channel chan interface{}
}

func (ec eventchannel) Channel() chan interface{} {
	return ec.channel
}

func BundleChannel(channel EventChannel) <-chan bundle.Bundle {
	ch := make(chan bundle.Bundle)
	go func() {
		for {
			bndl, ok := <-channel.Channel()
			if !ok {
				return
			}
			ch <- bndl.(bundle.Bundle)
		}
	}()
	return ch
}

func PromotionReattachmentChannel(channel EventChannel) <-chan PromotionReattachmentEvent {
	ch := make(chan PromotionReattachmentEvent)
	go func() {
		for {
			bndl, ok := <-channel.Channel()
			if !ok {
				return
			}
			ch <- bndl.(PromotionReattachmentEvent)
		}
	}()
	return ch
}

type AccountListener struct {
	Promotions       <-chan PromotionReattachmentEvent
	Reattachments    <-chan PromotionReattachmentEvent
	Sending          <-chan bundle.Bundle
	Sent             <-chan bundle.Bundle
	ReceivingDeposit <-chan bundle.Bundle
	ReceivedDeposit  <-chan bundle.Bundle
	ReceivedMessage  <-chan bundle.Bundle
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

type InputSelectionStrategyFunc func(a *Account, transferValue uint64, balanceCheck ...bool) (uint64, []api.Input, []uint64, error)

type addrindextuple struct {
	addr  Hash
	index uint64
}

type AccountsOpts struct {
	MWM                     uint64
	Depth                   uint64
	TransferPollInterval    uint64
	PromoteReattachInterval uint64
	SecurityLevel           consts.SecurityLevel
	Clock                   Clock
	ReceiveEventFilter      ReceiveEventFilter
	InputSelectionStrategy  InputSelectionStrategyFunc
}

func defaultAccountOpts(opts ...*AccountsOpts) *AccountsOpts {
	if len(opts) == 0 {
		return &AccountsOpts{
			MWM: 14, Depth: 3, SecurityLevel: consts.SecurityLevelMedium,
			TransferPollInterval: 10, PromoteReattachInterval: 10, Clock: &realclock{},
			ReceiveEventFilter:     NewPerTailReceiveEventFilter(),
			InputSelectionStrategy: LeftToRightInputSelection,
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
	opt.TransferPollInterval = defaultValue(opt.TransferPollInterval, 20)
	opt.PromoteReattachInterval = defaultValue(opt.TransferPollInterval, 20)
	if opt.Clock == nil {
		opt.Clock = &realclock{}
	}
	if opt.ReceiveEventFilter == nil {
		opt.ReceiveEventFilter = NewPerTailReceiveEventFilter()
	}
	if opt.InputSelectionStrategy == nil {
		opt.InputSelectionStrategy = LeftToRightInputSelection
	}
	return opt
}

func NewAccount(seed Trytes, storage Store, api *api.API, opts ...*AccountsOpts) (*Account, error) {
	opt := defaultAccountOpts(opts...)
	acc := &Account{
		id:           fmt.Sprintf("%x", md5.Sum([]byte(seed))),
		seed:         seed,
		request:      make(chan actionrequest),
		sendBackChan: make(chan actionresponse),
		exit:         make(chan struct{}),
		addrBuff:     make(chan addrindextuple, 5),
		errors:       make(chan ErrorEvent),
		api:          api,
		storage:      storage,
		listeners: map[AccountEvent][]EventChannel{
			EventSendingTransfer:   {},
			EventTransferConfirmed: {},
			EventReceivingDeposit:  {},
			EventReceivedDeposit:   {},
			EventReceivedMessage:   {},
			EventPromotion:         {},
			EventReattachment:      {},
		},
		inputSelectionStrat:     opt.InputSelectionStrategy,
		mwm:                     opt.MWM,
		depth:                   opt.Depth,
		secLvl:                  opt.SecurityLevel,
		transferPollInterval:    opt.TransferPollInterval,
		promoteReattachInterval: opt.PromoteReattachInterval,
		clock:                   opt.Clock,
		receiveEventFilter:      opt.ReceiveEventFilter,
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
	request      chan actionrequest
	sendBackChan chan actionresponse
	exit         chan struct{}

	// misc
	api     *api.API
	storage Store
	clock   Clock

	// addr
	addrFunc AddrFunc
	addrBuff chan addrindextuple

	// customization
	inputSelectionStrat     InputSelectionStrategyFunc
	mwm                     uint64
	depth                   uint64
	secLvl                  consts.SecurityLevel
	transferPollInterval    uint64
	promoteReattachInterval uint64

	// event
	eventsEnabled      bool
	receiveEventFilter ReceiveEventFilter
	listeners          map[AccountEvent][]EventChannel
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

// NewDepositRequest generates a new deposit request.
func (a *Account) NewDepositRequest(req *DepositRequest) (*DepositConditions, error) {
	if req.TimeoutOn == nil {
		return nil, ErrTimeoutNotSpecified
	}
	if req.TimeoutOn.Add(-(time.Duration(2) * time.Minute)).Before(time.Now()) {
		return nil, ErrTimeoutTooLow
	}
	a.request <- actionrequest{Action: action_new_deposit_address, Request: req}
	payload := <-a.sendBackChan
	if payload.err != nil {
		return nil, payload.err
	}
	return payload.item.(*DepositConditions), nil
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
func (a *Account) RegisterEventHandler(event AccountEvent) EventChannel {
	eventListeners := a.listeners[event]
	channel := eventchannel{make(chan interface{})}
	a.listeners[event] = append(eventListeners, channel)
	return channel
}

func ComposeEventListener(a *Account, events ...AccountEvent) AccountListener {
	listener := AccountListener{}
	for _, event := range events {
		switch event {
		case EventSendingTransfer:
			listener.Sending = BundleChannel(a.RegisterEventHandler(event))
		case EventTransferConfirmed:
			listener.Sent = BundleChannel(a.RegisterEventHandler(event))
		case EventReceivingDeposit:
			listener.ReceivingDeposit = BundleChannel(a.RegisterEventHandler(event))
		case EventReceivedDeposit:
			listener.ReceivedDeposit = BundleChannel(a.RegisterEventHandler(event))
		case EventReceivedMessage:
			listener.ReceivedMessage = BundleChannel(a.RegisterEventHandler(event))
		case EventPromotion:
			listener.Promotions = PromotionReattachmentChannel(a.RegisterEventHandler(event))
		case EventReattachment:
			listener.Reattachments = PromotionReattachmentChannel(a.RegisterEventHandler(event))
		}
	}
	return listener
}

func (a *Account) sendError(err error) {
	a.sendBackChan <- actionresponse{err: err}
}

func (a *Account) cleanup() {
	close(a.exit)
}

func (a *Account) runEventLoop() error {
	_, err := a.storage.LoadAccount(a.id)
	if err != nil {
		return err
	}

	// start deposit address generator
	addrFunc, err := a.newDepositAddressGenerator()
	if err != nil {
		return err
	}
	a.addrFunc = addrFunc

	// warm up the receive event filter by polling once before starting the event loop
	a.checkIncomingTransfers()
	a.eventsEnabled = true

	// TODO: when there are pending transfers in the database but not a single tail transaction
	// it means that the sending the transfer or storing the origin bundle tail hash failed.
	// thereby before the account event loop starts, the missing tail hash should be added.

	go func() {
		defer func() {
			if r := recover(); err != nil {
				a.cleanup()
				switch x := r.(type) {
				case ErrMarkDepositAddr:
					a.sendError(ErrAccountPanic{internalError: x})
				}
			}
		}()

		go func() {
			for {
				select {
				case <-time.After(time.Duration(a.transferPollInterval) * time.Second):
					a.pollTransfers()
				case <-a.exit:
					return
				}
			}
		}()

		go func() {
			for {
				select {
				case <-time.After(time.Duration(a.promoteReattachInterval) * time.Second):
					a.promoteAndReattach()
				case <-a.exit:
					return
				}
			}
		}()

		for {
			select {
			case req := <-a.request:
				switch req.Action {
				case action_send:
					if err := a.send(Recipients{req.Request.(Recipient)}); err != nil {
						a.sendError(err)
					}
				case action_multi_send:
					if err := a.send(req.Request.(Recipients)); err != nil {
						a.sendError(err)
					}
				case action_new_deposit_address:
					depReq := req.Request.(*DepositRequest)
					a.sendBackChan <- actionresponse{item: a.addrFunc(depReq)}
				case action_current_balance:
					balance, err := a.balance()
					a.sendBackChan <- actionresponse{item: balance, err: err}
				case action_trigger_transfer_polling:
					a.pollTransfers()
				case action_is_new:
					state, err := a.storage.LoadAccount(a.id)
					a.sendBackChan <- actionresponse{item: state.IsNew(), err: err}
				case action_shutdown:
					a.cleanup()
					a.sendBackChan <- actionresponse{}
					// exit event loop
					return
				}
			}
		}
	}()
	return nil
}

type AddrFunc func(dep *DepositRequest) *DepositConditions

func (a *Account) newDepositAddressGenerator() (AddrFunc, error) {
	state, err := a.storage.LoadAccount(a.id)
	if err != nil {
		return nil, err
	}

	// generating N addresses ahead into the buffer
	go func() {
		for index := state.KeyIndex + 1; ; index++ {
			addr, err := address.GenerateAddress(a.seed, index, a.secLvl, true)
			if err != nil {
				panic(err)
			}
			if err := a.storage.WriteIndex(a.id, index); err != nil {
				panic(err)
			}
			select {
			case a.addrBuff <- addrindextuple{addr, index}:
			case <-a.exit:
				return
			}
		}
	}()

	return func(req *DepositRequest) *DepositConditions {
		tuple := <-a.addrBuff
		if err := a.storage.AddDepositRequest(a.id, tuple.index, req); err != nil {
			panic(ErrMarkDepositAddr{err})
		}
		return &DepositConditions{Address: tuple.addr, DepositRequest: *req}
	}, nil
}

func (a *Account) emitEvent(payload interface{}, event AccountEvent) {
	// used during startup to flush events
	if !a.eventsEnabled {
		return
	}

	if event == EventError {
		a.errors <- payload.(ErrorEvent)
		return
	}

	eventListeners := a.listeners[event]
	for _, listener := range eventListeners {
		select {
		case listener.Channel() <- payload:
		default:
		}
	}
}

func (a *Account) send(targets Recipients) error {
	var inputs []api.Input
	var remainderAddress *Hash
	var err error

	transferSum := targets.Sum()
	forRemoval := []uint64{}
	if transferSum > 0 {
		// gather the total sum, inputs, addresses to remove from the store
		sum, ins, rem, err := a.inputSelectionStrat(a, transferSum)
		if err != nil {
			return err
		}
		inputs = ins
		forRemoval = rem

		// store and add remainder address to transfer
		if sum > transferSum {
			remainder := sum - transferSum
			depCond := a.addrFunc(&DepositRequest{ExpectedAmount: &remainder})
			remainderAddress = &depCond.Address
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
		return err
	}

	tips, err := a.api.GetTransactionsToApprove(a.depth)
	if err != nil {
		return err
	}

	powedTrytes, err := a.api.AttachToTangle(tips.TrunkTransaction, tips.BranchTransaction, a.mwm, bundleTrytes)
	if err != nil {
		return err
	}

	tailTx, err := transaction.AsTransactionObject(powedTrytes[0])
	if err != nil {
		return err
	}

	// add the new transfer to the db
	if err := a.storage.AddPendingTransfer(a.id, tailTx.Hash, powedTrytes, forRemoval...); err != nil {
		return err
	}

	bndlTrytes, err := a.api.StoreAndBroadcast(powedTrytes)
	if err != nil {
		return err
	}

	bndl, err := transaction.AsTransactionObjects(bndlTrytes, nil)
	if err != nil {
		return err
	}
	a.emitEvent(bndl, EventSendingTransfer)
	a.sendBackChan <- actionresponse{item: bndl}
	return nil
}

// LeftToRightInputSelection selects addresses as inputs which are deposit addresses,
// have no incoming transfers and have funds.
func LeftToRightInputSelection(a *Account, transferValue uint64, balanceCheck ...bool) (uint64, []api.Input, []uint64, error) {
	state, err := a.storage.LoadAccount(a.id)
	if err != nil {
		return 0, nil, nil, err
	}

	var balanceCheckOnly bool
	if len(balanceCheck) > 0 {
		balanceCheckOnly = true
	}

	// no deposit requests, therefore 0 balance
	if len(state.DepositRequests) == 0 && balanceCheckOnly {
		return 0, nil, nil, nil
	}

	if len(state.DepositRequests) == 0 {
		return 0, nil, nil, consts.ErrInsufficientBalance
	}

	now := time.Now()
	selected := []api.Input{}
	selectedTimedout := []uint64{}
	toRemove := []uint64{}

	addForRemove := func(keyIndex uint64) {
		if balanceCheckOnly {
			return
		}
		toRemove = append(toRemove, keyIndex)
	}

	selectIndex := func(keyIndex uint64, expectedAmount uint64) error {
		addr, _ := address.GenerateAddress(a.seed, keyIndex, a.secLvl, true)
		resp, err := a.api.GetBalances(Hashes{addr}, 100)
		if err != nil {
			return err
		}
		balance := resp.Balances[0]
		if balance < expectedAmount {
			return nil
		}
		selected = append(selected, api.Input{
			KeyIndex: keyIndex,
			Address:  addr,
			Security: a.secLvl,
			Balance:  balance,
		})
		return nil
	}

	for keyIndex, req := range state.DepositRequests {
		// remainder address
		if req.TimeoutOn == nil {
			if req.ExpectedAmount == nil {
				panic("remainder address in system without 'expected amount'")
			}
			if err := selectIndex(keyIndex, *req.ExpectedAmount); err != nil {
				return 0, nil, nil, err
			}
		}

		// timed out
		if now.After(*req.TimeoutOn) {
			selectedTimedout = append(selectedTimedout, keyIndex)
			continue
		}

		// multi
		if req.MultiUse {
			// multi use deposit addresses are only used
			// when they are timed out, if they don't define an expected amount
			if req.ExpectedAmount == nil {
				continue
			}
			if err := selectIndex(keyIndex, *req.ExpectedAmount); err != nil {
				return 0, nil, nil, err
			}
			continue
		}

		// single
		if req.ExpectedAmount == nil {
			addr, _ := address.GenerateAddress(a.seed, keyIndex, a.secLvl, true)
			resp, err := a.api.GetBalances(Hashes{addr}, 100)
			if err != nil {
				return 0, nil, nil, err
			}
			selected = append(selected, api.Input{
				Security: a.secLvl,
				Address:  addr,
				KeyIndex: keyIndex,
				Balance:  resp.Balances[0],
			})
			continue
		}

		if err := selectIndex(keyIndex, *req.ExpectedAmount); err != nil {
			return 0, nil, nil, err
		}
	}

	inputs := []api.Input{}
	addAsInput := func(input *api.Input) {
		if balanceCheckOnly {
			return
		}
		inputs = append(inputs, *input)
	}
	var sum uint64
	for i := range selected {
		input := &selected[i]
		sum += input.Balance
		addAsInput(input)
		if sum > transferValue && !balanceCheckOnly {
			break
		}
	}

	if sum < transferValue || balanceCheckOnly {
		for _, keyIndex := range selectedTimedout {
			addr, _ := address.GenerateAddress(a.seed, keyIndex, a.secLvl, true)

			// check whether has incoming consistent value transfer
			// and if so, don't use it in the input selection
			// (even though the address is timed out)
			var hasIncomingConsistentTransfer bool
			bndls, err := a.api.GetBundlesFromAddresses(Hashes{addr}, true)
			if err != nil {
				return 0, nil, nil, err
			}
			for i := range bndls {
				if *(bndls[i][0]).Persistence {
					continue
				}
				// check whether it's even a deposit to the address we are checking
				var isDepositToAddr bool
				for j := range bndls[i] {
					if bndls[i][j].Address == addr {
						if bndls[i][j].Value > 0 {
							isDepositToAddr = true
							break
						}
					}
				}

				// ignore this transfer as it isn't an incoming value transfer
				if !isDepositToAddr {
					continue
				}

				// here we have a bundle which is not yet confirmed
				// and is depositing something onto this address.
				// lets check it for consistency
				consistent, _, err := a.api.CheckConsistency(bndls[i][0].Hash)
				if err != nil {
					return 0, nil, nil, err
				}
				if consistent {
					hasIncomingConsistentTransfer = true
					break
				}
			}

			if hasIncomingConsistentTransfer {
				continue
			}

			resp, err := a.api.GetBalances(Hashes{addr}, 100)
			if err != nil {
				return 0, nil, nil, err
			}

			balance := resp.Balances[0]
			// remove if there's no incoming consistent transfer
			// and the balance is zero to free up the store
			if balance == 0 {
				if err := a.storage.RemoveDepositRequest(a.id, keyIndex); err != nil {
					a.emitEvent(ErrorEvent{Error: err, Type: ErrorInternal}, EventError)
				}
				continue
			}
			addForRemove(keyIndex)
			sum += balance
			addAsInput(&api.Input{
				KeyIndex: keyIndex,
				Address:  addr,
				Security: a.secLvl,
				Balance:  balance,
			})
			if sum > transferValue && !balanceCheckOnly {
				break
			}
		}
	}

	if balanceCheckOnly {
		return sum, nil, nil, nil
	}

	if sum < transferValue {
		return 0, nil, nil, consts.ErrInsufficientBalance
	}
	return sum, inputs, toRemove, nil
}

func (a *Account) balance() (uint64, error) {
	usableBalance, _, _, err := a.inputSelectionStrat(a, 0, true)
	return usableBalance, err
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
	for tailTx, pendingTransfer := range state.PendingTransfers {
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
				if err := a.storage.RemovePendingTransfer(a.id, tailTx); err != nil {
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
	if len(state.DepositRequests) == 0 {
		return
	}

	depositAddresses := make(Hashes, len(state.DepositRequests))
	i := 0
	for keyIndex := range state.DepositRequests {
		addr, err := address.GenerateAddress(a.seed, keyIndex, a.secLvl, true)
		if err != nil {
			panic(err)
		}
		depositAddresses[i] = addr
		i++
	}

	spentAddresses := Hashes{}
	for _, transfer := range state.PendingTransfers {
		bndl, err := essenceToBundle(transfer)
		if err != nil {
			panic(err)
		}
		for j := range bndl {
			if bndl[j].Value < 0 {
				spentAddresses = append(spentAddresses, bndl[j].Address)
			}
		}
	}

	// get all bundles which operated on the current deposit depsAddrs
	bndls, err := a.api.GetBundlesFromAddresses(depositAddresses, true)
	if err != nil {
		a.emitEvent(ErrorEvent{Error: err, Type: ErrorEventIncomingTransfers}, EventError)
		return
	}

	// create the events to emit in the event system
	for _, event := range a.receiveEventFilter.Filter(bndls, depositAddresses, spentAddresses) {
		a.emitEvent(event.Bundle, event.Event)
	}
}

const approxAboveMaxDepthMinutes = 5

func aboveMaxDepth(ts time.Time) bool {
	return time.Now().Sub(ts).Minutes() < approxAboveMaxDepthMinutes
}

const maxDepth = 15
const referenceToOldMsg = "reference transaction is too old"

var emptySeed = strings.Repeat("9", 81)
var ErrUnpromotableTail = errors.New("tail is unpromoteable")

func (a *Account) promoteAndReattach() {
	state, err := a.storage.LoadAccount(a.id)
	if err != nil {
		return
	}
	if len(state.PendingTransfers) == 0 {
		return
	}

	send := func(preparedBundle []Trytes, tips *api.TransactionsToApprove) (Hash, error) {
		readyBundle, err := a.api.AttachToTangle(tips.TrunkTransaction, tips.BranchTransaction, a.mwm, preparedBundle)
		if err != nil {
			return "", err
		}
		readyBundle, err = a.api.StoreAndBroadcast(readyBundle)
		if err != nil {
			return "", err
		}
		tailTx, err := transaction.AsTransactionObject(readyBundle[0])
		if err != nil {
			return "", err
		}
		return tailTx.Hash, nil
	}

	promote := func(tailTx Hash) (Hash, error) {
		depth := a.depth
		for {
			tips, err := a.api.GetTransactionsToApprove(depth, tailTx)
			if err != nil {
				if err.Error() == referenceToOldMsg {
					depth++
					if depth > maxDepth {
						return "", ErrUnpromotableTail
					}
					continue
				}
				return "", err
			}
			pTransfers := bundle.Transfers{bundle.EmptyTransfer}
			preparedBundle, err := a.api.PrepareTransfers(emptySeed, pTransfers, api.PrepareTransfersOptions{})
			if err != nil {
				return "", err
			}
			return send(preparedBundle, tips)
		}
	}

	reattach := func(essenceBndl bundle.Bundle) (Hash, error) {
		tips, err := a.api.GetTransactionsToApprove(a.depth)
		if err != nil {
			return "", err
		}
		essenceTrytes, err := transaction.TransactionsToTrytes(essenceBndl)
		if err != nil {
			return "", err
		}
		return send(essenceTrytes, tips)
	}

	storeTailTxHash := func(key string, tailTxHash string, msg string, event ErrorType) bool {
		if err := a.storage.AddTailHash(a.id, key, tailTxHash); err != nil {
			// might have been removed by polling goroutine
			if err == ErrPendingTransferNotFound {
				return true
			}
			a.emitEvent(ErrorEvent{Error: errors.Wrap(err, msg), Type: event}, EventError)
			return false
		}
		return true
	}

	for key, pendingTransfer := range state.PendingTransfers {
		// search for a tail transaction which is consistent and above max depth
		var tailToPromote string
		// go in reverse order to start from the most recent tails
		for i := len(pendingTransfer.Tails) - 1; i >= 0; i-- {
			tailTx := pendingTransfer.Tails[i]
			consistent, _, err := a.api.CheckConsistency(tailTx)
			if err != nil {
				continue
			}

			if !consistent {
				continue
			}

			txTrytes, err := a.api.GetTrytes(tailTx)
			if err != nil {
				continue
			}

			tx, err := transaction.AsTransactionObject(txTrytes[0])
			if err != nil {
				continue
			}

			if !aboveMaxDepth(time.Unix(int64(tx.Timestamp), 0)) {
				continue
			}

			tailToPromote = tailTx
			break
		}

		bndl, err := essenceToBundle(pendingTransfer)
		if err != nil {
			continue
		}

		// promote as a tail was found
		if len(tailToPromote) > 0 {
			promoteTailTxHash, err := promote(tailToPromote)
			if err != nil {
				a.emitEvent(ErrorEvent{Error: errors.Wrap(err, "unable to promote"), Type: ErrorPromoteTransfer}, EventError)
				continue
			}
			a.emitEvent(PromotionReattachmentEvent{
				BundleHash:          bndl[0].Bundle,
				PromotionTailTxHash: promoteTailTxHash,
				OriginTailTxHash:    key,
			}, EventPromotion)
			//storeTailTxHash(key, promoteTailTxHash, "unable to store promotion tx tail hash", ErrorPromoteTransfer)
			continue
		}

		// reattach
		reattachTailTxHash, err := reattach(bndl)
		if err != nil {
			a.emitEvent(ErrorEvent{Error: errors.Wrap(err, "unable to reattach bundle"), Type: ErrorReattachTransfer}, EventError)
			continue
		}
		a.emitEvent(PromotionReattachmentEvent{
			BundleHash:             bndl[0].Bundle,
			OriginTailTxHash:       key,
			ReattachmentTailTxHash: reattachTailTxHash,
		}, EventReattachment)
		if !storeTailTxHash(key, reattachTailTxHash, "unable to store reattachment tx tail hash", ErrorReattachTransfer) {
			continue
		}
		promoteTailTxHash, err := promote(reattachTailTxHash)
		if err != nil {
			a.emitEvent(ErrorEvent{Error: errors.Wrap(err, "unable to promote reattached bundle"), Type: ErrorPromoteTransfer}, EventError)
			continue
		}
		a.emitEvent(PromotionReattachmentEvent{
			BundleHash:          bndl[0].Bundle,
			OriginTailTxHash:    key,
			PromotionTailTxHash: promoteTailTxHash,
		}, EventPromotion)
		//storeTailTxHash(key, promoteTailTxHash, "unable to store promotion tx tail hash for reattachment", ErrorPromoteTransfer)
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
