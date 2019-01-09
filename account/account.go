package account

import (
	"crypto/sha256"
	"fmt"
	"github.com/iotaledger/iota.go/account/deposit"
	"github.com/iotaledger/iota.go/account/store"
	"github.com/iotaledger/iota.go/address"
	"github.com/iotaledger/iota.go/api"
	"github.com/iotaledger/iota.go/bundle"
	"github.com/iotaledger/iota.go/consts"
	"github.com/iotaledger/iota.go/guards"
	"github.com/iotaledger/iota.go/guards/validators"
	"github.com/iotaledger/iota.go/transaction"
	. "github.com/iotaledger/iota.go/trinary"
	"github.com/pkg/errors"
	"strings"
	"time"
)

type Clock interface {
	Now() (time.Time, error)
}

type systemclock struct{}

func (rc *systemclock) Now() (time.Time, error) {
	return time.Now().UTC(), nil
}

type action byte

const (
	actionSend action = iota
	actionNewDepositAddress
	actionUsableBalance
	actionTotalBalance
	actionIsNew
	actionTriggerTransferPolling
	actionUpdateComponents
	actionShutdown
)

type actionrequest struct {
	Action  action
	Request interface{}
}

// Recipient is a bundle.Transfer but with a nicer name.
type Recipient = bundle.Transfer
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

type actionresponse struct {
	item interface{}
	err  error
}

type InputSelectionStrategyFunc func(a *Account, transferValue uint64, balanceCheck bool) (uint64, []api.Input, []uint64, error)

type addrindextuple struct {
	addr  Hash
	index uint64
}

// Settings defines options when instantiating a new Account.
type Settings struct {
	// The minimum weight magnitude used to send transactions.
	MWM uint64
	// The depth used when searching for transactions to approve.
	Depth uint64
	// The interval at which in/outbound transfers are checked.
	TransferPollInterval uint64
	// The interval at which promotion and reattachments occur.
	PromoteReattachInterval uint64
	// The overall security level used by the account.
	SecurityLevel consts.SecurityLevel
	// The clock to use to get time information. This field should only be set explicitly in testing environments.
	Clock Clock
	// A filter which takes care of filtering incoming transfer events.
	ReceiveEventFilter ReceiveEventFilter
	// The input selection strategy used to determine inputs and usable balance.
	InputSelectionStrategy InputSelectionStrategyFunc
	// Whether to emit events for incoming transfers in the first transfer polling.
	// This option should not be set to true, if the lib user is only interested into
	// transfers happening against deposit addresses, after account instantiation.
	EmitFirstTransferPollingEvents bool
}

func defaultAccountOpts(opts ...*Settings) *Settings {
	if len(opts) == 0 {
		return &Settings{
			MWM: 14, Depth: 3, SecurityLevel: consts.SecurityLevelMedium,
			TransferPollInterval: 10, PromoteReattachInterval: 10, Clock: &systemclock{},
			ReceiveEventFilter:     NewPerTailReceiveEventFilter(),
			InputSelectionStrategy: DefaultInputSelection,
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
	opt.PromoteReattachInterval = defaultValue(opt.PromoteReattachInterval, 20)
	if opt.Clock == nil {
		opt.Clock = &systemclock{}
	}
	if opt.ReceiveEventFilter == nil {
		opt.ReceiveEventFilter = NewPerTailReceiveEventFilter()
	}
	if opt.InputSelectionStrategy == nil {
		opt.InputSelectionStrategy = DefaultInputSelection
	}
	return opt
}

// NewAccount creates a new account with the given seed, under the given store, using the given API.
func NewAccount(seed Trytes, storage store.Store, api *api.API, opts ...*Settings) (*Account, error) {
	if err := validators.Validate(validators.ValidateSeed(seed)); err != nil {
		return nil, err
	}
	opt := defaultAccountOpts(opts...)
	acc := &Account{
		id:           fmt.Sprintf("%x", sha256.Sum256([]byte(seed))),
		seed:         seed,
		request:      make(chan actionrequest),
		sendBackChan: make(chan actionresponse),
		exit:         make(chan struct{}),
		addrBuff:     make(chan addrindextuple, 5),
		errors:       make(chan ErrorEvent),
		api:          api,
		store:        storage,
		listeners: map[AccountEvent][]EventChannel{
			EventSendingTransfer:   {},
			EventTransferConfirmed: {},
			EventReceivingDeposit:  {},
			EventReceivedDeposit:   {},
			EventReceivedMessage:   {},
			EventPromotion:         {},
			EventReattachment:      {},
		},
		setts:         opt,
		eventsEnabled: opt.EmitFirstTransferPollingEvents,
	}
	if err := acc.runEventLoop(); err != nil {
		return nil, err
	}
	return acc, nil
}

// Account is a thread-safe object encapsulating address management, input selection, promotion and reattachments.
type Account struct {
	errors chan ErrorEvent

	id   string
	seed Trytes

	// internal event loop
	request      chan actionrequest
	sendBackChan chan actionresponse
	exit         chan struct{}

	// misc
	api   *api.API
	store store.Store

	// addr
	addrFunc AddrFunc
	addrBuff chan addrindextuple

	// customization
	setts *Settings

	// event
	eventsEnabled         bool
	listeners             map[AccountEvent][]EventChannel
	firstTransferPollMade bool
}

// Send sends the specified amounts to the given recipients.
func (acc *Account) Send(recipients ...Recipient) (bundle.Bundle, error) {
	if recipients == nil || len(recipients) == 0 {
		return nil, ErrEmptyRecipients
	}
	for _, target := range recipients {
		if !guards.IsTrytesOfExactLength(target.Address, consts.HashTrytesSize+consts.AddressChecksumTrytesSize) {
			return nil, consts.ErrInvalidAddress
		}
	}

	acc.request <- actionrequest{Action: actionSend, Request: recipients}
	payload := <-acc.sendBackChan
	if payload.err != nil {
		return nil, payload.err
	}
	return payload.item.(bundle.Bundle), nil
}

// NewDepositRequest generates a new deposit request.
func (acc *Account) NewDepositRequest(req *deposit.Request) (*deposit.Conditions, error) {
	if req.TimeoutOn == nil {
		return nil, ErrTimeoutNotSpecified
	}
	currentTime, err := acc.setts.Clock.Now()
	if err != nil {
		return nil, err
	}
	if req.TimeoutOn.Add(-(time.Duration(2) * time.Minute)).Before(currentTime) {
		return nil, ErrTimeoutTooLow
	}
	acc.request <- actionrequest{Action: actionNewDepositAddress, Request: req}
	payload := <-acc.sendBackChan
	if payload.err != nil {
		return nil, payload.err
	}
	return payload.item.(*deposit.Conditions), nil
}

// UsableBalance gets the current usable balance.
// The balance is computed from all current deposit addresses which are ready
// for input selection. To get the current total balance, use TotalBalance().
func (acc *Account) UsableBalance() (uint64, error) {
	acc.request <- actionrequest{Action: actionUsableBalance}
	payload := <-acc.sendBackChan
	if payload.err != nil {
		return 0, payload.err
	}
	return payload.item.(uint64), nil
}

// TotalBalance gets the current total balance.
// The total balance is computed from all currently allocated deposit addresses.
// It does not represent the actual usable balance for doing transfers.
// Use UsableBalance() to get the current usable balance.
func (acc *Account) TotalBalance() (uint64, error) {
	acc.request <- actionrequest{Action: actionTotalBalance}
	payload := <-acc.sendBackChan
	if payload.err != nil {
		return 0, payload.err
	}
	return payload.item.(uint64), nil
}

// IsNew checks whether the account is new.
func (acc *Account) IsNew() (bool, error) {
	acc.request <- actionrequest{Action: actionIsNew}
	payload := <-acc.sendBackChan
	if payload.err != nil {
		return false, payload.err
	}
	return payload.item.(bool), nil
}

// Shutdown cleanly shutdowns the account and releases its goroutines.
func (acc *Account) Shutdown(dontAwaitBackgroundTasks ...bool) error {
	acc.request <- actionrequest{Action: actionShutdown, Request: dontAwaitBackgroundTasks}
	return (<-acc.sendBackChan).err
}

// Errors returns a channel from which errors can be received from internal processes (in-/out going transfer polling, promotion/reattachment etc.).
func (acc *Account) Errors() <-chan ErrorEvent {
	return acc.errors
}

// TriggerTransferPolling triggers a transfer polling.
func (acc *Account) TriggerTransferPolling() {
	acc.request <- actionrequest{Action: actionTriggerTransferPolling}
}

type componentchange struct {
	setts *Settings
	store store.Store
	api   *api.API
}

func (sc *componentchange) apply(a *Account) {
	if sc.setts != nil {
		a.setts = sc.setts
	}
	if sc.store != nil {
		a.store = sc.store
	}
	if sc.api != nil {
		a.api = sc.api
	}
}

// UpdateComponents updates the components of the account in a safe and synchronized manner.
// Each component to change is optional.
func (acc *Account) UpdateComponents(setts *Settings, store store.Store, api *api.API) error {
	acc.request <- actionrequest{Action: actionUpdateComponents, Request: &componentchange{
		setts, store, api,
	}}
	return (<-acc.sendBackChan).err
}

// RegisterEventHandler registers a new event handler.
func (acc *Account) RegisterEventHandler(event AccountEvent) EventChannel {
	eventListeners := acc.listeners[event]
	channel := eventchannel{make(chan interface{})}
	acc.listeners[event] = append(eventListeners, channel)
	return channel
}

func (acc *Account) sendError(err error) {
	acc.sendBackChan <- actionresponse{err: err}
}

func (acc *Account) cleanup() {
	close(acc.exit)
}

func (acc *Account) runEventLoop() error {
	_, err := acc.store.LoadAccount(acc.id)
	if err != nil {
		return err
	}

	// start deposit address generator
	addrFunc, err := acc.newDepositAddressGenerator()
	if err != nil {
		return err
	}
	acc.addrFunc = addrFunc

	// warm up the receive event filter by polling once before starting the event loop
	go acc.checkIncomingTransfers()

	go func() {
		defer func() {
			if r := recover(); err != nil {
				acc.cleanup()
				switch x := r.(type) {
				case ErrMarkDepositAddr:
					acc.sendError(ErrAccountPanic{internalError: x})
				}
			}
		}()

		transferPollingSync := acc.runTransferPolling()
		promReattachSync := acc.runPromoterReattacher()

		var cleanShutdown bool
		var dontAwaitBackgroundTasks bool
	exit:
		for {
			select {
			case req := <-acc.request:
				switch req.Action {
				case actionSend:
					bndl, err := acc.send(req.Request.([]bundle.Transfer))
					acc.sendBackChan <- actionresponse{item: bndl, err: err}
				case actionNewDepositAddress:
					depReq := req.Request.(*deposit.Request)
					acc.sendBackChan <- actionresponse{item: acc.addrFunc(depReq)}
				case actionUsableBalance:
					balance, err := acc.usableBalance()
					acc.sendBackChan <- actionresponse{item: balance, err: err}
				case actionTotalBalance:
					balance, err := acc.totalBalance()
					acc.sendBackChan <- actionresponse{item: balance, err: err}
				case actionTriggerTransferPolling:
					if acc.firstTransferPollMade {
						acc.pollTransfers()
					}
				case actionIsNew:
					state, err := acc.store.LoadAccount(acc.id)
					acc.sendBackChan <- actionresponse{item: state.IsNew(), err: err}
				case actionUpdateComponents:
					// await current ongoing polling goroutines to finish
					<-transferPollingSync
					<-promReattachSync
					// update the settings of the account
					req.Request.(*componentchange).apply(acc)
					// continue polling goroutines
					transferPollingSync <- struct{}{}
					promReattachSync <- struct{}{}
				case actionShutdown:
					acc.cleanup()
					dontAwaitBackgroundTasks = req.Request.(bool)
					cleanShutdown = true
					// exit event loop
					break exit
				}
			}
		}

		if cleanShutdown {
			if !dontAwaitBackgroundTasks {
				acc.sendBackChan <- actionresponse{}
				return
			}
			// await interval based goroutines to finish
			<-transferPollingSync
			<-promReattachSync
			acc.sendBackChan <- actionresponse{}
		}
	}()
	return nil
}

func (acc *Account) runPromoterReattacher() chan struct{} {
	promReattachSync := make(chan struct{})
	go func() {
	exit:
		for {
			select {
			case <-time.After(time.Duration(acc.setts.PromoteReattachInterval) * time.Second):
				acc.promoteAndReattach()
				select {
				case <-acc.exit:
					break exit
				default:
					select {
					case promReattachSync <- struct{}{}:
						<-promReattachSync
					default:
					}
				}
			case <-acc.exit:
				break exit
			}
		}
		close(promReattachSync)
	}()
	return promReattachSync
}

func (acc *Account) runTransferPolling() chan struct{} {
	transferPollingSync := make(chan struct{})
	go func() {
	exit:
		for {
			select {
			case <-time.After(time.Duration(acc.setts.TransferPollInterval) * time.Second):
				if acc.firstTransferPollMade {
					acc.pollTransfers()
					select {
					case <-acc.exit:
						break exit
					default:
						select {
						case transferPollingSync <- struct{}{}:
							<-transferPollingSync
						default:
						}
					}
				}
			case <-acc.exit:
				break exit
			}
		}
		close(transferPollingSync)
	}()
	return transferPollingSync
}

type AddrFunc func(dep *deposit.Request) *deposit.Conditions

func (acc *Account) newDepositAddressGenerator() (AddrFunc, error) {
	state, err := acc.store.LoadAccount(acc.id)
	if err != nil {
		return nil, err
	}

	// generating N addresses ahead into the buffer
	go func() {
		for index := state.KeyIndex + 1; ; index++ {
			addr, err := address.GenerateAddress(acc.seed, index, acc.setts.SecurityLevel, true)
			if err != nil {
				panic(err)
			}
			if err := acc.store.WriteIndex(acc.id, index); err != nil {
				panic(err)
			}
			select {
			case acc.addrBuff <- addrindextuple{addr, index}:
			case <-acc.exit:
				return
			}
		}
	}()

	return func(req *deposit.Request) *deposit.Conditions {
		tuple := <-acc.addrBuff
		if err := acc.store.AddDepositRequest(acc.id, tuple.index, req); err != nil {
			panic(ErrMarkDepositAddr{err})
		}
		return &deposit.Conditions{Address: tuple.addr, Request: *req}
	}, nil
}

func (acc *Account) emitEvent(payload interface{}, event AccountEvent) {
	// used during startup to flush events
	if !acc.eventsEnabled {
		return
	}

	if event == EventError {
		acc.errors <- payload.(ErrorEvent)
		return
	}

	eventListeners := acc.listeners[event]
	for _, listener := range eventListeners {
		select {
		case listener.Channel() <- payload:
		default:
		}
	}
}

func (acc *Account) send(targets Recipients) (bundle.Bundle, error) {
	var inputs []api.Input
	var remainderAddress *Hash
	var err error
	transferSum := targets.Sum()
	forRemoval := []uint64{}
	if transferSum > 0 {
		// gather the total sum, inputs, addresses to remove from the store
		sum, ins, rem, err := acc.setts.InputSelectionStrategy(acc, transferSum, false)
		if err != nil {
			return nil, errors.Wrap(err, "failed to perform input selection in send op.")
		}

		inputs = ins
		forRemoval = rem

		// store and add remainder address to transfer
		if sum > transferSum {
			remainder := sum - transferSum
			depCond := acc.addrFunc(&deposit.Request{ExpectedAmount: &remainder})
			remainderAddress = &depCond.Address
		}

		// TODO: maybe check for target address spent state?
	}

	transfers := targets.AsTransfers()

	currentTime, err := acc.setts.Clock.Now()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get current time in send op.")
	}

	ts := uint64(currentTime.UnixNano() / int64(time.Second))
	opts := api.PrepareTransfersOptions{
		Inputs:           inputs,
		RemainderAddress: remainderAddress,
		Security:         acc.setts.SecurityLevel,
		Timestamp:        &ts,
	}
	bundleTrytes, err := acc.api.PrepareTransfers(acc.seed, transfers, opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to prepare transfers in send op.")
	}

	tips, err := acc.api.GetTransactionsToApprove(acc.setts.Depth)
	if err != nil {
		return nil, errors.Wrap(err, "unable to GTTA in send op.")
	}

	powedTrytes, err := acc.api.AttachToTangle(tips.TrunkTransaction, tips.BranchTransaction, acc.setts.MWM, bundleTrytes)
	if err != nil {
		return nil, errors.Wrap(err, "performing PoW in send op. failed")
	}

	tailTx, err := transaction.AsTransactionObject(powedTrytes[0])
	if err != nil {
		return nil, err
	}

	// add the new transfer to the db
	if err := acc.store.AddPendingTransfer(acc.id, tailTx.Hash, powedTrytes, forRemoval...); err != nil {
		return nil, errors.Wrap(err, "unable to store pending transfer in send op.")
	}

	bndlTrytes, err := acc.api.StoreAndBroadcast(powedTrytes)
	if err != nil {
		return nil, errors.Wrap(err, "unable to store/broadcast bundle in send op.")
	}

	bndl, err := transaction.AsTransactionObjects(bndlTrytes, nil)
	if err != nil {
		return nil, err
	}

	acc.emitEvent(bndl, EventSendingTransfer)
	return bndl, nil
}

// DefaultInputSelection selects fulfilled and timed out deposit addresses as inputs.
func DefaultInputSelection(a *Account, transferValue uint64, balanceCheck bool) (uint64, []api.Input, []uint64, error) {
	state, err := a.store.LoadAccount(a.id)
	if err != nil {
		return 0, nil, nil, errors.Wrap(err, "unable to load account state for input selection")
	}

	// no deposit requests, therefore 0 balance
	if len(state.DepositRequests) == 0 && balanceCheck {
		return 0, nil, nil, nil
	}

	if len(state.DepositRequests) == 0 {
		return 0, nil, nil, consts.ErrInsufficientBalance
	}

	// get the current solid subtangle milestone for doing each getBalance query with the same milestone
	solidSubtangleMilestone, err := a.api.GetLatestSolidSubtangleMilestone()
	if err != nil {
		return 0, nil, nil, errors.Wrap(err, "unable to fetch latest solid subtangle milestone for input selection")
	}
	subtangleHash := solidSubtangleMilestone.LatestSolidSubtangleMilestone

	now, err := a.setts.Clock.Now()
	if err != nil {
		return 0, nil, nil, errors.Wrap(err, "unable to get time for doing input selection")
	}

	type selection struct {
		keyIndex       uint64
		expectedAmount *uint64
	}

	// addresses to query on the first pass
	primaryAddrs := Hashes{}

	// selected addresses
	selections := []selection{}

	// addresses which are timed out
	// (generation of address is deferred up until it's needed)
	timedOutAddrs := []uint64{}

	// addresses/indices to remove from the store
	toRemove := []uint64{}

	addForRemove := func(keyIndex uint64) {
		if balanceCheck {
			return
		}
		toRemove = append(toRemove, keyIndex)
	}

	// iterate over all allocated deposit addresses
	for keyIndex, req := range state.DepositRequests {
		// remainder address
		if req.TimeoutOn == nil {
			if req.ExpectedAmount == nil {
				panic("remainder address in system without 'expected amount'")
			}
			addr, _ := address.GenerateAddress(a.seed, keyIndex, a.setts.SecurityLevel, true)
			primaryAddrs = append(primaryAddrs, addr)
			selections = append(selections, selection{keyIndex, req.ExpectedAmount})
			continue
		}

		// timed out
		if now.After(*req.TimeoutOn) {
			timedOutAddrs = append(timedOutAddrs, keyIndex)
			continue
		}

		// multi
		if req.MultiUse {
			// multi use deposit addresses are only used
			// when they are timed out, if they don't define an expected amount
			if req.ExpectedAmount == nil {
				continue
			}
			addr, _ := address.GenerateAddress(a.seed, keyIndex, a.setts.SecurityLevel, true)
			primaryAddrs = append(primaryAddrs, addr)
			selections = append(selections, selection{keyIndex, req.ExpectedAmount})
			continue
		}

		// single
		addr, _ := address.GenerateAddress(a.seed, keyIndex, a.setts.SecurityLevel, true)
		primaryAddrs = append(primaryAddrs, addr)
		selections = append(selections, selection{keyIndex, req.ExpectedAmount})
	}

	// get the balance of all selected addresses in one go
	balances, err := a.api.GetBalances(primaryAddrs, 100, subtangleHash)
	if err != nil {
		return 0, nil, nil, errors.Wrap(err, "unable to fetch balances of primary selected addresses for input selection")
	}

	inputs := []api.Input{}
	addAsInput := func(input *api.Input) {
		if balanceCheck {
			return
		}
		inputs = append(inputs, *input)
	}

	// add addresses as inputs which fulfill their criteria
	var sum uint64
	for i := range selections {
		s := &selections[i]
		// skip addresses which have an expected amount which isn't reached however
		if s.expectedAmount != nil && balances.Balances[i] < *s.expectedAmount {
			continue
		}
		sum += balances.Balances[i]

		// add the address as an input
		if balances.Balances[i] > 0 {
			addAsInput(&api.Input{
				Address:  primaryAddrs[i],
				KeyIndex: s.keyIndex,
				Balance:  balances.Balances[i],
				Security: a.setts.SecurityLevel,
			})
		}

		// mark the address for removal as it should be freed from the store
		addForRemove(s.keyIndex)
		if sum > transferValue && !balanceCheck {
			break
		}
	}

	// if we didn't fulfill the transfer value,
	// lets use the timed out addresses too to try to fulfill the transfer
	if sum < transferValue || balanceCheck {
		for _, keyIndex := range timedOutAddrs {
			addr, _ := address.GenerateAddress(a.seed, keyIndex, a.setts.SecurityLevel, true)

			// check whether the timed out address has an incoming consistent value transfer,
			// and if so, don't use it in the input selection
			if has, err := a.hasIncomingConsistentTransfer(addr); has || err != nil {
				continue
			}

			resp, err := a.api.GetBalances(Hashes{addr}, 100, subtangleHash)
			if err != nil {
				return 0, nil, nil, errors.Wrapf(err, "unable to fetch balance of timed out address %s (key index %d) for input selection", addr, keyIndex)
			}

			balance := resp.Balances[0]
			// remove if there's no incoming consistent transfer
			// and the balance is zero to free up the store
			if balance == 0 {
				if err := a.store.RemoveDepositRequest(a.id, keyIndex); err != nil {
					a.emitEvent(ErrorEvent{Error: err, Type: ErrorInternal}, EventError)
				}
				continue
			}
			addForRemove(keyIndex)
			sum += balance
			addAsInput(&api.Input{
				KeyIndex: keyIndex,
				Address:  addr,
				Security: a.setts.SecurityLevel,
				Balance:  balance,
			})
			if sum > transferValue && !balanceCheck {
				break
			}
		}
	}

	if balanceCheck {
		return sum, nil, nil, nil
	}

	if sum < transferValue {
		return 0, nil, nil, consts.ErrInsufficientBalance
	}
	return sum, inputs, toRemove, nil
}

func (acc *Account) hasIncomingConsistentTransfer(addr Hash) (bool, error) {
	var has bool
	bndls, err := acc.api.GetBundlesFromAddresses(Hashes{addr}, true)
	if err != nil {
		return false, err
	}
	for i := range bndls {
		if *(bndls[i][0]).Persistence {
			continue
		}
		// check whether it's even acc deposit to the address we are checking
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

		// here we have acc bundle which is not yet confirmed
		// and is depositing something onto this address.
		// lets check it for consistency
		hash := bndls[i][0].Hash
		consistent, _, err := acc.api.CheckConsistency(hash)
		if err != nil {
			return false, errors.Wrapf(err, "unable to check consistency of tx %s in incoming consistent transfer check", hash)
		}
		if consistent {
			has = true
			break
		}
	}
	return has, nil
}

func (acc *Account) usableBalance() (uint64, error) {
	balance, _, _, err := acc.setts.InputSelectionStrategy(acc, 0, true)
	return balance, err
}

func (acc *Account) totalBalance() (uint64, error) {
	state, err := acc.store.LoadAccount(acc.id)
	if err != nil {
		return 0, errors.Wrap(err, "unable to load account state for querying total balance")
	}

	depositReqsCount := len(state.DepositRequests)
	if depositReqsCount == 0 {
		return 0, nil
	}

	solidSubtangleMilestone, err := acc.api.GetLatestSolidSubtangleMilestone()
	if err != nil {
		return 0, errors.Wrap(err, "unable to fetch latest solid subtangle milestone for querying total balance")
	}
	subtangleHash := solidSubtangleMilestone.LatestSolidSubtangleMilestone

	addrs := make(Hashes, len(state.DepositRequests))
	var i int
	for keyIndex, _ := range state.DepositRequests {
		addr, _ := address.GenerateAddress(acc.seed, keyIndex, acc.setts.SecurityLevel, true)
		addrs[i] = addr
		i++
	}

	balances, err := acc.api.GetBalances(addrs, 100, subtangleHash)
	if err != nil {
		return 0, errors.Wrap(err, "unable to fetch balances for computing total balance")
	}
	var sum uint64
	for _, balance := range balances.Balances {
		sum += balance
	}

	return sum, nil
}

func (acc *Account) hasIncomingTransfer(index uint64) (bool, error) {
	addr, err := address.GenerateAddress(acc.seed, index, acc.setts.SecurityLevel, true)
	if err != nil {
		return false, err
	}

	bundles, err := acc.api.GetBundlesFromAddresses(Hashes{addr}, true)
	if err != nil {
		return false, err
	}

	for _, bndl := range bundles {
		// if the bundle is invalid don't consider it at all as an incoming transfer
		if err := bundle.ValidBundle(bndl); err != nil {
			continue
		}
		// if the bundle is not acc value transfer don't consider it either
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

func (acc *Account) pollTransfers() {
	acc.checkOutgoingTransfers()
	acc.checkIncomingTransfers()
}

func (acc *Account) checkOutgoingTransfers() {
	state, _ := acc.store.LoadAccount(acc.id)
	for tailTx, pendingTransfer := range state.PendingTransfers {
		if len(pendingTransfer.Tails) == 0 {
			continue
		}
		states, err := acc.api.GetLatestInclusion(pendingTransfer.Tails)
		if err != nil {
			acc.emitEvent(ErrorEvent{Error: err, Type: ErrorEventOutgoingTransfers}, EventError)
			return
		}
		// if any state is true we can remove the transfer as it got confirmed
		for i, state := range states {
			if state {
				bndl, err := acc.api.GetBundle(pendingTransfer.Tails[i])
				if err != nil {
					acc.emitEvent(ErrorEvent{Error: err, Type: ErrorEventOutgoingTransfers}, EventError)
					return
				}
				acc.emitEvent(bndl, EventTransferConfirmed)
				if err := acc.store.RemovePendingTransfer(acc.id, tailTx); err != nil {
					acc.emitEvent(ErrorEvent{Error: err, Type: ErrorEventOutgoingTransfers}, EventError)
					return
				}
				break
			}
		}
	}
}

func (acc *Account) checkIncomingTransfers() {
	defer func() {
		if !acc.firstTransferPollMade {
			acc.firstTransferPollMade = true
			acc.eventsEnabled = true
		}
	}()

	state, _ := acc.store.LoadAccount(acc.id)
	if len(state.DepositRequests) == 0 {
		return
	}

	depositAddresses := make(Hashes, len(state.DepositRequests))
	i := 0
	for keyIndex := range state.DepositRequests {
		addr, err := address.GenerateAddress(acc.seed, keyIndex, acc.setts.SecurityLevel, true)
		if err != nil {
			panic(err)
		}
		depositAddresses[i] = addr
		i++
	}

	spentAddresses := Hashes{}
	for _, transfer := range state.PendingTransfers {
		bndl, err := store.PendingTransferToBundle(transfer)
		if err != nil {
			panic(err)
		}
		for j := range bndl {
			if bndl[j].Value < 0 {
				spentAddresses = append(spentAddresses, bndl[j].Address)
			}
		}
	}

	// get all bundles which operated on the current deposit addresses
	bndls, err := acc.api.GetBundlesFromAddresses(depositAddresses, true)
	if err != nil {
		acc.emitEvent(ErrorEvent{Error: err, Type: ErrorEventIncomingTransfers}, EventError)
		return
	}

	// create the events to emit in the event system
	for _, event := range acc.setts.ReceiveEventFilter.Filter(bndls, depositAddresses, spentAddresses) {
		acc.emitEvent(event.Bundle, event.Event)
	}
}

const approxAboveMaxDepthMinutes = 5

func aboveMaxDepth(clock Clock, ts time.Time) (bool, error) {
	currentTime, err := clock.Now()
	if err != nil {
		return false, err
	}
	return currentTime.Sub(ts).Minutes() < approxAboveMaxDepthMinutes, nil
}

const maxDepth = 15
const referenceToOldMsg = "reference transaction is too old"

var emptySeed = strings.Repeat("9", 81)
var ErrUnpromotableTail = errors.New("tail is unpromoteable")

func (acc *Account) promoteAndReattach() {
	state, err := acc.store.LoadAccount(acc.id)
	if err != nil {
		return
	}
	if len(state.PendingTransfers) == 0 {
		return
	}

	send := func(preparedBundle []Trytes, tips *api.TransactionsToApprove) (Hash, error) {
		readyBundle, err := acc.api.AttachToTangle(tips.TrunkTransaction, tips.BranchTransaction, acc.setts.MWM, preparedBundle)
		if err != nil {
			return "", errors.Wrap(err, "performing PoW for promote/reattach cycle bundle failed")
		}
		readyBundle, err = acc.api.StoreAndBroadcast(readyBundle)
		if err != nil {
			return "", errors.Wrap(err, "unable to store/broadcast bundle in promote/reattach cycle")
		}
		tailTx, err := transaction.AsTransactionObject(readyBundle[0])
		if err != nil {
			return "", err
		}
		return tailTx.Hash, nil
	}

	promote := func(tailTx Hash) (Hash, error) {
		depth := acc.setts.Depth
		for {
			tips, err := acc.api.GetTransactionsToApprove(depth, tailTx)
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
			preparedBundle, err := acc.api.PrepareTransfers(emptySeed, pTransfers, api.PrepareTransfersOptions{})
			if err != nil {
				return "", errors.Wrap(err, "unable to prepare promotion bundle")
			}
			return send(preparedBundle, tips)
		}
	}

	reattach := func(essenceBndl bundle.Bundle) (Hash, error) {
		tips, err := acc.api.GetTransactionsToApprove(acc.setts.Depth)
		if err != nil {
			return "", errors.Wrapf(err, "unable to GTTA for reattachment in promote/reattach cycle (bundle %s)", essenceBndl[0].Bundle)
		}
		essenceTrytes, err := transaction.TransactionsToTrytes(essenceBndl)
		if err != nil {
			return "", err
		}
		return send(essenceTrytes, tips)
	}

	storeTailTxHash := func(key string, tailTxHash string, msg string, event ErrorType) bool {
		if err := acc.store.AddTailHash(acc.id, key, tailTxHash); err != nil {
			// might have been removed by polling goroutine
			if err == store.ErrPendingTransferNotFound {
				return true
			}
			acc.emitEvent(ErrorEvent{Error: errors.Wrap(err, msg), Type: event}, EventError)
			return false
		}
		return true
	}

	for key, pendingTransfer := range state.PendingTransfers {
		// search for acc tail transaction which is consistent and above max depth
		var tailToPromote string
		// go in reverse order to start from the most recent tails
		for i := len(pendingTransfer.Tails) - 1; i >= 0; i-- {
			tailTx := pendingTransfer.Tails[i]
			consistent, _, err := acc.api.CheckConsistency(tailTx)
			if err != nil {
				continue
			}

			if !consistent {
				continue
			}

			txTrytes, err := acc.api.GetTrytes(tailTx)
			if err != nil {
				continue
			}

			tx, err := transaction.AsTransactionObject(txTrytes[0])
			if err != nil {
				continue
			}

			if above, err := aboveMaxDepth(acc.setts.Clock, time.Unix(int64(tx.Timestamp), 0)); !above || err != nil {
				continue
			}

			tailToPromote = tailTx
			break
		}

		bndl, err := store.PendingTransferToBundle(pendingTransfer)
		if err != nil {
			continue
		}

		// promote as acc tail was found
		if len(tailToPromote) > 0 {
			promoteTailTxHash, err := promote(tailToPromote)
			if err != nil {
				acc.emitEvent(ErrorEvent{Error: errors.Wrap(err, "unable to promote"), Type: ErrorPromoteTransfer}, EventError)
				continue
			}
			acc.emitEvent(PromotionReattachmentEvent{
				BundleHash:          bndl[0].Bundle,
				PromotionTailTxHash: promoteTailTxHash,
				OriginTailTxHash:    key,
			}, EventPromotion)
			continue
		}

		// reattach
		reattachTailTxHash, err := reattach(bndl)
		if err != nil {
			acc.emitEvent(ErrorEvent{Error: errors.Wrap(err, "unable to reattach bundle"), Type: ErrorReattachTransfer}, EventError)
			continue
		}
		acc.emitEvent(PromotionReattachmentEvent{
			BundleHash:             bndl[0].Bundle,
			OriginTailTxHash:       key,
			ReattachmentTailTxHash: reattachTailTxHash,
		}, EventReattachment)
		if !storeTailTxHash(key, reattachTailTxHash, "unable to store reattachment tx tail hash", ErrorReattachTransfer) {
			continue
		}
		promoteTailTxHash, err := promote(reattachTailTxHash)
		if err != nil {
			acc.emitEvent(ErrorEvent{Error: errors.Wrap(err, "unable to promote reattached bundle"), Type: ErrorPromoteTransfer}, EventError)
			continue
		}
		acc.emitEvent(PromotionReattachmentEvent{
			BundleHash:          bndl[0].Bundle,
			OriginTailTxHash:    key,
			PromotionTailTxHash: promoteTailTxHash,
		}, EventPromotion)
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
