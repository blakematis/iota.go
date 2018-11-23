package account

import (
	"crypto/md5"
	"fmt"
	"github.com/iotaledger/iota.go/address"
	"github.com/iotaledger/iota.go/api"
	"github.com/iotaledger/iota.go/bundle"
	"github.com/iotaledger/iota.go/consts"
	"github.com/iotaledger/iota.go/guards"
	. "github.com/iotaledger/iota.go/trinary"
	"time"
)

type Clock interface {
	Now() time.Time
}

type realclock struct{}

func (rc *realclock) Now() time.Time {
	return time.Now()
}

type AccountEvent byte

const (
	// fired when a transfer was broadcasted
	EventSendingTransfer AccountEvent = iota
	// fired when a broadcasted transfer got confirmed
	EventTransferConfirmed
	// fired when a deposit is being received
	EventReceivingDeposit
	// fired when a deposit is confirmed
	EventReceivedDeposit
	// fired when a zero value transaction is received
	EventReceivedMessage
)

type EventChannel chan<- interface{}
type BundleEventChannel chan bundle.Bundle

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
		id:                  fmt.Sprintf("%x", md5.Sum([]byte(seed))),
		seed:                seed,
		sendChan:            make(chan sendmsg),
		sendBackChan:        make(chan backmsg),
		receiveChan:         make(chan sendmsg),
		newDepAddrChan:      make(chan struct{}),
		lastDepAddrChan:     make(chan struct{}),
		balanceReqChan:      make(chan struct{}),
		newReqChan:          make(chan struct{}),
		transferPollingChan: make(chan struct{}),
		api:                 api,
		storage:             storage,
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
	if err := acc.run(); err != nil {
		return nil, err
	}
	return acc, nil
}

type sendmsg struct {
	target string
	amount uint64
}

type backmsg struct {
	item interface{}
	err  error
}

type Account struct {
	id   string
	seed Trytes

	// clockwork
	sendChan            chan sendmsg
	sendBackChan        chan backmsg
	receiveChan         chan sendmsg
	newDepAddrChan      chan struct{}
	lastDepAddrChan     chan struct{}
	balanceReqChan      chan struct{}
	newReqChan          chan struct{}
	transferPollingChan chan struct{}

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

func (a *Account) Send(target Hash, amount uint64) (bundle.Bundle, error) {
	if !guards.IsTrytesOfExactLength(target, consts.HashTrytesSize+consts.AddressChecksumTrytesSize) {
		return nil, consts.ErrInvalidAddress
	}
	a.sendChan <- sendmsg{target: target, amount: amount}
	payload := <-a.sendBackChan
	if payload.err != nil {
		return nil, payload.err
	}
	return payload.item.(bundle.Bundle), nil
}

func (a *Account) NewDepositAddress() (Hash, error) {
	a.newDepAddrChan <- struct{}{}
	payload := <-a.sendBackChan
	if payload.err != nil {
		return "", payload.err
	}
	return payload.item.(Hash), nil
}

func (a *Account) LastDepositAddress() (Hash, error) {
	a.lastDepAddrChan <- struct{}{}
	payload := <-a.sendBackChan
	if payload.err != nil {
		return "", payload.err
	}
	return payload.item.(Hash), nil
}

func (a *Account) Balance() (uint64, error) {
	a.balanceReqChan <- struct{}{}
	payload := <-a.sendBackChan
	if payload.err != nil {
		return 0, payload.err
	}
	return payload.item.(uint64), nil
}

func (a *Account) IsNew() (bool, error) {
	a.newReqChan <- struct{}{}
	payload := <-a.sendBackChan
	if payload.err != nil {
		return false, payload.err
	}
	return payload.item.(bool), nil
}

func (a *Account) TriggerTransferPolling() {
	a.transferPollingChan <- struct{}{}
}

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
	a.sendBackChan <- backmsg{err: err}
}

func (a *Account) run() error {
	_, err := a.storage.LoadAccount(a.id)
	if err != nil {
		return err
	}
	// warm up the receive event filter by polling once before starting the clockwork
	a.checkIncomingTransfers()
	a.eventsEnabled = true
	go func() {
		for {
			select {
			case req := <-a.sendChan:
				a.send(req)
			case <-a.receiveChan:
			case <-a.newDepAddrChan:
				addr, err := a.newDepositAddress()
				if err != nil {
					a.sendError(err)
					continue
				}
				a.sendBackChan <- backmsg{item: addr}
			case <-a.lastDepAddrChan:
				addr, err := a.lastDepositAddress()
				if err != nil {
					a.sendError(err)
					continue
				}
				a.sendBackChan <- backmsg{item: addr}
			case <-a.balanceReqChan:
				balance, err := a.balance()
				if err != nil {
					a.sendError(err)
					continue
				}
				a.sendBackChan <- backmsg{item: balance}
			case <-a.newReqChan:
				state, err := a.storage.LoadAccount(a.id)
				if err != nil {
					a.sendError(err)
					continue
				}
				a.sendBackChan <- backmsg{item: state.IsNew()}
			case <-a.transferPollingChan:
				a.pollTransfers()
				// no fallthrough possible in selects
			case <-time.After(time.Duration(a.transferPollInterval) * time.Second):
				a.pollTransfers()
			}
		}
	}()
	return nil
}

func (a *Account) fireEvent(payload interface{}, event AccountEvent) {
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
	}
}

func (a *Account) send(req sendmsg) {
	var inputs []api.Input
	var remainderAddress *Hash
	var sum uint64
	var err error
	if req.amount > 0 {
		sum, inputs, err = a.selectInputs(req.amount)
		if err != nil {
			a.sendError(err)
			return
		}
		// generate a remainder address for the transfer
		// by adding it to the store
		if sum > req.amount {
			addr, err := a.newDepositAddress()
			if err != nil {
				a.sendError(err)
				return
			}
			remainderAddress = &addr
		}

		// TODO: maybe check for target address spent state?
	}

	transfers := bundle.Transfers{
		{
			Value:   req.amount,
			Address: req.target,
		},
	}

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

	// TODO: pending transfer and address marking must be done atomically

	// add the new transfer to the db
	if err := a.storage.AddPendingTransfer(a.id, bundleTrytes); err != nil {
		a.sendError(err)
		return
	}

	// mark the used addresses as spent
	if inputs != nil {
		indices := make([]uint64, len(inputs))
		for i, input := range inputs {
			indices[i] = input.KeyIndex
		}
		if err := a.storage.MarkSpentAddresses(a.id, indices...); err != nil {
			a.sendError(err)
			return
		}
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

	a.fireEvent(bndl, EventSendingTransfer)
	a.sendBackChan <- backmsg{item: bndl}
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

func (a *Account) newDepositAddress() (Hash, error) {
	state, err := a.storage.LoadAccount(a.id)
	if err != nil {
		return "", err
	}
	nextIndex := state.lastKeyIndex + 1
	depositAddr, err := address.GenerateAddress(a.seed, nextIndex, a.secLvl, true)
	if err != nil {
		return "", err
	}
	if err := a.storage.MarkDepositAddresses(a.id, nextIndex); err != nil {
		return "", err
	}
	return depositAddr, nil
}

func (a *Account) lastDepositAddress() (Hash, error) {
	state, err := a.storage.LoadAccount(a.id)
	if err != nil {
		return "", err
	}
	depositAddresses := state.DepositAddresses()
	if len(depositAddresses) == 0 {
		return a.newDepositAddress()
	}
	index := depositAddresses[len(depositAddresses)-1]
	depositAddr, err := address.GenerateAddress(a.seed, index, a.secLvl, true)
	if err != nil {
		return "", err
	}
	return depositAddr, nil
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
			// TODO: maybe fire error event?
			return
		}
		// if any state is true we can remove the transfer as it got confirmed
		for i, state := range states {
			if state {
				bndl, err := a.api.GetBundle(pendingTransfer.Tails[i])
				if err != nil {
					// TODO: maybe fire error event?
					return
				}
				a.fireEvent(bndl, EventTransferConfirmed)
				a.storage.RemovePendingTransfer(a.id, bndl[0].Bundle)
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
		// TODO: maybe fire error event?
		return
	}

	// create the events to fire in the event system
	for _, event := range a.receiveEventFilter.Filter(bndls, depsAddrs, spentAddrs) {
		a.fireEvent(event.Bundle, event.Event)
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

	// filter out bundles for which a previous event was fired
	// and fire new events for the new bundles
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
