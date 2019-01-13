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
	"sync"
	"time"
)

// Account is a thread-safe object encapsulating address management, input selection, promotion and reattachments.
type Account interface {
	// Start starts the inner event loop of the account.
	Start() error
	// Shutdown cleanly shutdowns the account and releases its allocated resources.
	Shutdown(dontAwaitBackgroundTasks ...bool) error
	// Send sends the specified amounts to the given recipients.
	Send(recipients ...Recipient) (bundle.Bundle, error)
	// NewDepositRequest generates a new deposit request.
	NewDepositRequest(req *deposit.Request) (*deposit.Conditions, error)
	// UsableBalance gets the current usable balance.
	// The balance is computed from all current deposit addresses which are ready
	// for input selection. To get the current total balance, use TotalBalance().
	UsableBalance() (uint64, error)
	// TotalBalance gets the current total balance.
	// The total balance is computed from all currently allocated deposit addresses.
	// It does not represent the actual usable balance for doing transfers.
	// Use UsableBalance() to get the current usable balance.
	TotalBalance() (uint64, error)
	// IsNew checks whether the account is new.
	IsNew() (bool, error)
	// TriggerTransferPolling awaits the current transfer polling task
	// to finish (in case it's ongoing), pauses the task, does a manual transfer polling,
	// resumes the transfer poll task and then returns.
	TriggerTransferPolling()
	// UpdateSettings updates the settings of the account in a safe and synchronized manner.
	UpdateSettings(setts *Settings) error
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
	actionExecuteSync
)

type actionrequest struct {
	Action  action
	Request interface{}
}

// Recipient is a bundle.Transfer but with a nicer name.
type Recipient = bundle.Transfer
type Recipients []Recipient

// Sum returns the sum of all amounts.
func (recps Recipients) Sum() uint64 {
	var sum uint64
	for _, target := range recps {
		sum += target.Value
	}
	return sum
}

// AsTransfers converts the recipients to transfers.
func (recps Recipients) AsTransfers() bundle.Transfers {
	transfers := make(bundle.Transfers, len(recps))
	for i, recipient := range recps {
		transfers[i] = recipient
	}
	return transfers
}

// response from a processed account request, may contain an error.
type actionresponse struct {
	item interface{}
	err  error
}

// tuple of an address and its index.
type addrindextuple struct {
	addr          Hash
	index         uint64
	securityLevel consts.SecurityLevel
}

func newAccount(setts *Settings) (Account, error) {
	seed, err := setts.seedProv.Seed()
	if err != nil {
		return nil, err
	}
	if err := validators.Validate(validators.ValidateSeed(seed)); err != nil {
		return nil, err
	}
	return &account{
		id:           fmt.Sprintf("%x", sha256.Sum256([]byte(seed))),
		request:      make(chan actionrequest),
		sendBackChan: make(chan actionresponse),
		exit:         make(chan struct{}),
		addrBuff:     make(chan addrindextuple, 5),
		setts:        setts,
	}, nil
}

type account struct {
	id string

	// customization
	setts *Settings

	// internal event loop
	request      chan actionrequest
	sendBackChan chan actionresponse
	exit         chan struct{}

	// addr
	addrFunc AddrFunc
	addrBuff chan addrindextuple
}

func (acc *account) Send(recipients ...Recipient) (bundle.Bundle, error) {
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

func (acc *account) NewDepositRequest(req *deposit.Request) (*deposit.Conditions, error) {
	if req.TimeoutOn == nil {
		return nil, ErrTimeoutNotSpecified
	}
	currentTime, err := acc.setts.clock.Now()
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

func (acc *account) UsableBalance() (uint64, error) {
	acc.request <- actionrequest{Action: actionUsableBalance}
	payload := <-acc.sendBackChan
	if payload.err != nil {
		return 0, payload.err
	}
	return payload.item.(uint64), nil
}

func (acc *account) TotalBalance() (uint64, error) {
	acc.request <- actionrequest{Action: actionTotalBalance}
	payload := <-acc.sendBackChan
	if payload.err != nil {
		return 0, payload.err
	}
	return payload.item.(uint64), nil
}

func (acc *account) IsNew() (bool, error) {
	acc.request <- actionrequest{Action: actionIsNew}
	payload := <-acc.sendBackChan
	if payload.err != nil {
		return false, payload.err
	}
	return payload.item.(bool), nil
}

func (acc *account) Shutdown(dontAwaitBackgroundTasks ...bool) error {
	acc.request <- actionrequest{Action: actionShutdown, Request: dontAwaitBackgroundTasks}
	return (<-acc.sendBackChan).err
}

func (acc *account) TriggerTransferPolling() {
	acc.request <- actionrequest{Action: actionTriggerTransferPolling}
	<-acc.sendBackChan
}

func (acc *account) UpdateSettings(setts *Settings) error {
	acc.request <- actionrequest{Action: actionUpdateComponents, Request: setts}
	return (<-acc.sendBackChan).err
}

type syncfunc func() error

// executes the given function within the account's event loop to ensure fully
// synchronous execution where no other account action can happen at the same time.
func (acc *account) executeSync(f syncfunc) error {
	acc.request <- actionrequest{Action: actionExecuteSync, Request: f}
	return (<-acc.sendBackChan).err
}

func (acc *account) sendError(err error) {
	acc.sendBackChan <- actionresponse{err: err}
}

func (acc *account) cleanup() {
	close(acc.exit)
}

func (acc *account) Start() error {
	_, err := acc.setts.store.LoadAccount(acc.id)
	if err != nil {
		return err
	}

	// start deposit address generator
	addrFunc, err := acc.newDepositAddressGenerator()
	if err != nil {
		return err
	}
	acc.addrFunc = addrFunc

	// account event loop
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

		transferPolling := acc.runTransferPolling()
		promoterReattacher := acc.runPromoterReattacher()

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
					// await current transfer polling to finish
					transferPolling.Pause()
					acc.pollTransfers()
					acc.sendBackChan <- actionresponse{}
					// continue transfer polling
					transferPolling.Resume()

				case actionIsNew:
					state, err := acc.setts.store.LoadAccount(acc.id)
					acc.sendBackChan <- actionresponse{item: state.IsNew(), err: err}

				case actionUpdateComponents:

					// await current ongoing polling goroutines to finish
					transferPolling.Pause()
					promoterReattacher.Pause()

					// update the settings of the account
					newSetts, ok := req.Request.(*Settings)
					if newSetts != nil && ok {
						// make a copy
						settingsCopy := *newSetts
						acc.setts = &settingsCopy
					}

					// continue polling goroutines
					transferPolling.Resume()
					promoterReattacher.Resume()
				case actionExecuteSync:
					f, ok := req.Request.(syncfunc)
					if !ok {
						continue
					}
					acc.sendBackChan <- actionresponse{err: f()}
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
			// await interval based goroutines to finish.
			// Pause() will automatically return when the goroutines terminated
			transferPolling.Pause()
			promoterReattacher.Pause()
			acc.sendBackChan <- actionresponse{}
			acc.setts.eventMachine.Emit(struct{}{}, EventShutdown)
		}
	}()
	return nil
}

type tasksyncer chan struct{}

func (ts tasksyncer) Pause() {
	<-ts
}

func (ts tasksyncer) Resume() {
	ts <- struct{}{}
}

func (acc *account) runPromoterReattacher() tasksyncer {
	taskSyncer := make(tasksyncer)
	go func() {
	exit:
		for {
			select {
			case <-time.After(acc.setts.promoteReattachInterval):
				acc.setts.promoteReattachmentStrategy(acc)
				select {
				case <-acc.exit:
					break exit
				default:
				}
				// check for pause signal
			case taskSyncer <- struct{}{}:
				// await resume signal
				<-taskSyncer
			case <-acc.exit:
				break exit
			}
		}
		close(taskSyncer)
	}()
	return taskSyncer
}

func (acc *account) runTransferPolling() tasksyncer {
	taskSyncer := make(tasksyncer)
	go func() {
	exit:
		for {
			select {
			case <-time.After(acc.setts.transferPollInterval):
				acc.pollTransfers()
				select {
				case <-acc.exit:
					break exit
				default:
				}
				// check pause signal
			case taskSyncer <- struct{}{}:
				// await resume signal
				<-taskSyncer
			case <-acc.exit:
				break exit
			}
		}
		close(taskSyncer)
	}()
	return taskSyncer
}

// AddrFunc is a function which takes in a deposit request and creates
// an object describing the conditions for the deposit.
// An AddrFunc is not thread-safe.
type AddrFunc func(dep *deposit.Request) *deposit.Conditions

func (acc *account) newDepositAddressGenerator() (AddrFunc, error) {
	state, err := acc.setts.store.LoadAccount(acc.id)
	if err != nil {
		return nil, err
	}

	seed, err := acc.setts.seedProv.Seed()
	if err != nil {
		return nil, err
	}

	// generating N addresses ahead into the buffer
	go func() {
		for index := state.KeyIndex + 1; ; index++ {
			// copy
			secLevl := acc.setts.securityLevel
			addr, err := address.GenerateAddress(seed, index, secLevl, true)
			if err != nil {
				panic(err)
			}
			if err := acc.setts.store.WriteIndex(acc.id, index); err != nil {
				panic(err)
			}
			select {
			case acc.addrBuff <- addrindextuple{addr, index, secLevl}:
			case <-acc.exit:
				return
			}
		}
	}()

	return func(req *deposit.Request) *deposit.Conditions {
		tuple := <-acc.addrBuff
		// the address generation goroutine might have created addresses
		// into the channel which are not of the current desired security level because
		// it's not synchronized with settings updates.
		// lets simply skip addresses which do not fulfill the currently set security level.
		// note that the security level can't be changed in the meantime inside this check as settings updates
		// happen in a synchronized fashion inside the action processing event loop.
		for {
			if tuple.securityLevel != acc.setts.securityLevel {
				tuple = <-acc.addrBuff
				continue
			}
			break
		}
		storedReq := &store.StoredDepositRequest{SecurityLevel: tuple.securityLevel, Request: *req}
		if err := acc.setts.store.AddDepositRequest(acc.id, tuple.index, storedReq); err != nil {
			panic(ErrMarkDepositAddr{err})
		}
		return &deposit.Conditions{Address: tuple.addr, Request: *req}
	}, nil
}

func (acc *account) send(targets Recipients) (bundle.Bundle, error) {
	var inputs []api.Input
	var remainderAddress *Hash
	var err error
	transferSum := targets.Sum()
	forRemoval := []uint64{}

	if transferSum > 0 {
		// gather the total sum, inputs, addresses to remove from the store
		sum, ins, rem, err := acc.setts.inputSelectionStrategy(acc, transferSum, false)
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
	}

	transfers := targets.AsTransfers()
	currentTime, err := acc.setts.clock.Now()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get current time in send op.")
	}

	ts := uint64(currentTime.UnixNano() / int64(time.Second))
	opts := api.PrepareTransfersOptions{
		Inputs:           inputs,
		RemainderAddress: remainderAddress,
		Timestamp:        &ts,
	}

	seed, err := acc.setts.seedProv.Seed()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get seed from seed provider in send op.")
	}

	bundleTrytes, err := acc.setts.api.PrepareTransfers(seed, transfers, opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to prepare transfers in send op.")
	}

	tips, err := acc.setts.api.GetTransactionsToApprove(acc.setts.depth)
	if err != nil {
		return nil, errors.Wrap(err, "unable to GTTA in send op.")
	}

	powedTrytes, err := acc.setts.api.AttachToTangle(tips.TrunkTransaction, tips.BranchTransaction, acc.setts.mwm, bundleTrytes)
	if err != nil {
		return nil, errors.Wrap(err, "performing PoW in send op. failed")
	}

	tailTx, err := transaction.AsTransactionObject(powedTrytes[0])
	if err != nil {
		return nil, err
	}

	// add the new transfer to the db
	if err := acc.setts.store.AddPendingTransfer(acc.id, tailTx.Hash, powedTrytes, forRemoval...); err != nil {
		return nil, errors.Wrap(err, "unable to store pending transfer in send op.")
	}

	bndlTrytes, err := acc.setts.api.StoreAndBroadcast(powedTrytes)
	if err != nil {
		return nil, errors.Wrap(err, "unable to store/broadcast bundle in send op.")
	}

	bndl, err := transaction.AsTransactionObjects(bndlTrytes, nil)
	if err != nil {
		return nil, err
	}

	acc.setts.eventMachine.Emit(bndl, EventSendingTransfer)
	return bndl, nil
}

// selects fulfilled and timed out deposit addresses as inputs.
func defaultInputSelectionStrategy(acc *account, transferValue uint64, balanceCheck bool) (uint64, []api.Input, []uint64, error) {
	state, err := acc.setts.store.LoadAccount(acc.id)
	if err != nil {
		return 0, nil, nil, errors.Wrap(err, "unable to load account state for input selection")
	}

	// no deposit requests, therefore 0 balance
	if len(state.DepositRequests) == 0 {
		if balanceCheck {
			return 0, nil, nil, nil
		}
		// we can't fulfill any transfer value if we have no deposit requests
		return 0, nil, nil, consts.ErrInsufficientBalance
	}

	// get the current solid subtangle milestone for doing each getBalance query with the same milestone
	solidSubtangleMilestone, err := acc.setts.api.GetLatestSolidSubtangleMilestone()
	if err != nil {
		return 0, nil, nil, errors.Wrap(err, "unable to fetch latest solid subtangle milestone for input selection")
	}
	subtangleHash := solidSubtangleMilestone.LatestSolidSubtangleMilestone

	now, err := acc.setts.clock.Now()
	if err != nil {
		return 0, nil, nil, errors.Wrap(err, "unable to get time for doing input selection")
	}

	type selection struct {
		keyIndex uint64
		req      *store.StoredDepositRequest
	}

	// addresses to query on the first pass
	primaryAddrs := Hashes{}

	// selected addresses
	selections := []selection{}

	// addresses which are timed out
	// (generation of address is deferred up until it's needed)
	timedOutAddrs := []selection{}

	// addresses/indices to remove from the store
	toRemove := []uint64{}

	addForRemoval := func(keyIndex uint64) {
		if balanceCheck {
			return
		}
		toRemove = append(toRemove, keyIndex)
	}

	seed, err := acc.setts.seedProv.Seed()
	if err != nil {
		return 0, nil, nil, errors.Wrap(err, "unable to get seed from seed provider for doing input selection")
	}

	// iterate over all allocated deposit addresses
	for keyIndex, req := range state.DepositRequests {
		// remainder address
		if req.TimeoutOn == nil {
			if req.ExpectedAmount == nil {
				panic("remainder address in system without 'expected amount'")
			}
			addr, _ := address.GenerateAddress(seed, keyIndex, req.SecurityLevel, true)
			primaryAddrs = append(primaryAddrs, addr)
			selections = append(selections, selection{keyIndex, req})
			continue
		}

		// timed out
		if now.After(*req.TimeoutOn) {
			timedOutAddrs = append(timedOutAddrs, selection{keyIndex, req})
			continue
		}

		// multi
		if req.MultiUse {
			// multi use deposit addresses are only used
			// when they are timed out, if they don't define an expected amount
			if req.ExpectedAmount == nil {
				continue
			}
			addr, _ := address.GenerateAddress(seed, keyIndex, req.SecurityLevel, true)
			primaryAddrs = append(primaryAddrs, addr)
			selections = append(selections, selection{keyIndex, req})
			continue
		}

		// single
		addr, _ := address.GenerateAddress(seed, keyIndex, req.SecurityLevel, true)
		primaryAddrs = append(primaryAddrs, addr)
		selections = append(selections, selection{keyIndex, req})
	}

	// get the balance of all selected addresses in one go
	balances, err := acc.setts.api.GetBalances(primaryAddrs, 100, subtangleHash)
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
		if s.req.ExpectedAmount != nil && balances.Balances[i] < *s.req.ExpectedAmount {
			continue
		}
		sum += balances.Balances[i]

		// add the address as an input
		if balances.Balances[i] > 0 {
			addAsInput(&api.Input{
				Address:  primaryAddrs[i],
				KeyIndex: s.keyIndex,
				Balance:  balances.Balances[i],
				Security: s.req.SecurityLevel,
			})
		}

		// mark the address for removal as it should be freed from the store
		addForRemoval(s.keyIndex)
		if sum > transferValue && !balanceCheck {
			break
		}
	}

	// if we didn't fulfill the transfer value,
	// lets use the timed out addresses too to try to fulfill the transfer
	if sum < transferValue || balanceCheck {
		for i := range timedOutAddrs {
			s := &timedOutAddrs[i]
			addr, _ := address.GenerateAddress(seed, s.keyIndex, s.req.SecurityLevel, true)

			// check whether the timed out address has an incoming consistent value transfer,
			// and if so, don't use it in the input selection
			if has, err := acc.hasIncomingConsistentTransfer(addr); has || err != nil {
				continue
			}

			resp, err := acc.setts.api.GetBalances(Hashes{addr}, 100, subtangleHash)
			if err != nil {
				return 0, nil, nil, errors.Wrapf(err, "unable to fetch balance of timed out address %s (key index %d) for input selection", addr, s.keyIndex)
			}

			balance := resp.Balances[0]
			// remove if there's no incoming consistent transfer
			// and the balance is zero to free up the store
			if balance == 0 {
				if err := acc.setts.store.RemoveDepositRequest(acc.id, s.keyIndex); err != nil {
					acc.setts.eventMachine.Emit(ErrorEvent{Error: err, Type: ErrorInternal}, EventError)
				}
				continue
			}
			addForRemoval(s.keyIndex)
			sum += balance
			addAsInput(&api.Input{
				KeyIndex: s.keyIndex,
				Address:  addr,
				Security: s.req.SecurityLevel,
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

func (acc *account) hasIncomingConsistentTransfer(addr Hash) (bool, error) {
	var has bool
	bndls, err := acc.setts.api.GetBundlesFromAddresses(Hashes{addr}, true)
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
		consistent, _, err := acc.setts.api.CheckConsistency(hash)
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

func (acc *account) usableBalance() (uint64, error) {
	balance, _, _, err := acc.setts.inputSelectionStrategy(acc, 0, true)
	return balance, err
}

func (acc *account) totalBalance() (uint64, error) {
	state, err := acc.setts.store.LoadAccount(acc.id)
	if err != nil {
		return 0, errors.Wrap(err, "unable to load account state for querying total balance")
	}

	depositReqsCount := len(state.DepositRequests)
	if depositReqsCount == 0 {
		return 0, nil
	}

	solidSubtangleMilestone, err := acc.setts.api.GetLatestSolidSubtangleMilestone()
	if err != nil {
		return 0, errors.Wrap(err, "unable to fetch latest solid subtangle milestone for querying total balance")
	}
	subtangleHash := solidSubtangleMilestone.LatestSolidSubtangleMilestone

	seed, err := acc.setts.seedProv.Seed()
	if err != nil {
		return 0, errors.Wrap(err, "unable to get seed from seed provider for computing total balance")
	}

	addrs := make(Hashes, len(state.DepositRequests))
	var i int
	for keyIndex, req := range state.DepositRequests {
		addr, _ := address.GenerateAddress(seed, keyIndex, req.SecurityLevel, true)
		addrs[i] = addr
		i++
	}

	balances, err := acc.setts.api.GetBalances(addrs, 100, subtangleHash)
	if err != nil {
		return 0, errors.Wrap(err, "unable to fetch balances for computing total balance")
	}
	var sum uint64
	for _, balance := range balances.Balances {
		sum += balance
	}

	return sum, nil
}

func (acc *account) pollTransfers() {
	pendingTransfers, err := acc.setts.store.GetPendingTransfers(acc.id)
	if err != nil {
		acc.setts.eventMachine.Emit(ErrorEvent{
			Error: errors.Wrap(err, "unable to load account state for polling transfers"),
			Type:  ErrorEventOutgoingTransfers}, EventError)
		return
	}

	depositRequests, err := acc.setts.store.GetDepositRequests(acc.id)
	if err != nil {
		acc.setts.eventMachine.Emit(ErrorEvent{
			Error: errors.Wrap(err, "unable to load account state for polling transfers"),
			Type:  ErrorEventOutgoingTransfers}, EventError)
		return
	}

	// poll incoming/outgoing in parallel
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		acc.checkOutgoingTransfers(pendingTransfers)
	}()
	go func() {
		defer wg.Done()
		acc.checkIncomingTransfers(depositRequests, pendingTransfers)
	}()
	wg.Wait()
}

func (acc *account) checkOutgoingTransfers(pendingTransfers map[string]*store.PendingTransfer) {
	for tailTx, pendingTransfer := range pendingTransfers {
		if len(pendingTransfer.Tails) == 0 {
			continue
		}
		states, err := acc.setts.api.GetLatestInclusion(pendingTransfer.Tails)
		if err != nil {
			acc.setts.eventMachine.Emit(ErrorEvent{
				Error: errors.Wrapf(err, "unable to check latest inclusion state in outgoing transfers op. (first tail tx of bundle: %s)", tailTx),
				Type:  ErrorEventOutgoingTransfers}, EventError)
			return
		}
		// if any state is true we can remove the transfer as it got confirmed
		for i, state := range states {
			if !state {
				continue
			}
			// fetch bundle to emit it in the event
			bndl, err := acc.setts.api.GetBundle(pendingTransfer.Tails[i])
			if err != nil {
				acc.setts.eventMachine.Emit(ErrorEvent{
					Error: errors.Wrapf(err, "unable to get bundle in outgoing transfers op. (first tail tx of bundle: %s) of tail %s", tailTx, pendingTransfer.Tails[i]),
					Type:  ErrorEventOutgoingTransfers}, EventError)
				return
			}
			acc.setts.eventMachine.Emit(bndl, EventTransferConfirmed)
			if err := acc.setts.store.RemovePendingTransfer(acc.id, tailTx); err != nil {
				acc.setts.eventMachine.Emit(ErrorEvent{
					Error: errors.Wrap(err, "unable to remove confirmed transfer from store in outgoing transfers op."),
					Type:  ErrorEventOutgoingTransfers}, EventError)
				return
			}
			break
		}
	}
}

func (acc *account) checkIncomingTransfers(depositRequests map[uint64]*store.StoredDepositRequest, pendingTransfers map[string]*store.PendingTransfer) {
	if len(depositRequests) == 0 {
		return
	}

	seed, err := acc.setts.seedProv.Seed()
	if err != nil {
		acc.setts.eventMachine.Emit(ErrorEvent{
			Error: errors.Wrap(err, "unable to get seed from seed provider in incoming transfers op."),
			Type:  ErrorInternal},
			EventError)
		return
	}

	depositAddresses := make(StringSet)
	depAddrs := make(Hashes, len(depositRequests))
	var i int
	for keyIndex, req := range depositRequests {
		addr, err := address.GenerateAddress(seed, keyIndex, req.SecurityLevel, true)
		if err != nil {
			acc.setts.eventMachine.Emit(ErrorEvent{
				Error: errors.Wrap(err, "unable to compute deposit address in incoming transfers op."),
				Type:  ErrorEventIncomingTransfers}, EventError)
		}
		depAddrs[i] = addr
		i++
		depositAddresses[addr] = struct{}{}
	}

	spentAddresses := make(StringSet)
	for _, transfer := range pendingTransfers {
		bndl, err := store.PendingTransferToBundle(transfer)
		if err != nil {
			panic(err)
		}
		for j := range bndl {
			if bndl[j].Value < 0 {
				spentAddresses[bndl[j].Address] = struct{}{}
			}
		}
	}

	// get all bundles which operated on the current deposit addresses
	bndls, err := acc.setts.api.GetBundlesFromAddresses(depAddrs, true)
	if err != nil {
		acc.setts.eventMachine.Emit(ErrorEvent{
			Error: errors.Wrap(err, "unable to fetch bundles from deposit addresses in incoming transfers op."),
			Type:  ErrorEventIncomingTransfers}, EventError)
		return
	}

	// create the events to emit in the event system
	acc.setts.receiveEventFilter(acc.setts.eventMachine, bndls, depositAddresses, spentAddresses)
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

func defaultPromoteReattachmentStrategy(acc *account) {
	state, err := acc.setts.store.LoadAccount(acc.id)
	if err != nil {
		return
	}
	if len(state.PendingTransfers) == 0 {
		return
	}

	send := func(preparedBundle []Trytes, tips *api.TransactionsToApprove) (Hash, error) {
		readyBundle, err := acc.setts.api.AttachToTangle(tips.TrunkTransaction, tips.BranchTransaction, acc.setts.mwm, preparedBundle)
		if err != nil {
			return "", errors.Wrap(err, "performing PoW for promote/reattach cycle bundle failed")
		}
		readyBundle, err = acc.setts.api.StoreAndBroadcast(readyBundle)
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
		depth := acc.setts.depth
		for {
			tips, err := acc.setts.api.GetTransactionsToApprove(depth, tailTx)
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
			preparedBundle, err := acc.setts.api.PrepareTransfers(emptySeed, pTransfers, api.PrepareTransfersOptions{})
			if err != nil {
				return "", errors.Wrap(err, "unable to prepare promotion bundle")
			}
			return send(preparedBundle, tips)
		}
	}

	reattach := func(essenceBndl bundle.Bundle) (Hash, error) {
		tips, err := acc.setts.api.GetTransactionsToApprove(acc.setts.depth)
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
		if err := acc.setts.store.AddTailHash(acc.id, key, tailTxHash); err != nil {
			// might have been removed by polling goroutine
			if err == store.ErrPendingTransferNotFound {
				return true
			}
			acc.setts.eventMachine.Emit(ErrorEvent{Error: errors.Wrap(err, msg), Type: event}, EventError)
			return false
		}
		return true
	}

	for key, pendingTransfer := range state.PendingTransfers {
		// search for tail transaction which is consistent and above max depth
		var tailToPromote string
		// go in reverse order to start from the most recent tails
		for i := len(pendingTransfer.Tails) - 1; i >= 0; i-- {
			tailTx := pendingTransfer.Tails[i]
			consistent, _, err := acc.setts.api.CheckConsistency(tailTx)
			if err != nil {
				continue
			}

			if !consistent {
				continue
			}

			txTrytes, err := acc.setts.api.GetTrytes(tailTx)
			if err != nil {
				continue
			}

			tx, err := transaction.AsTransactionObject(txTrytes[0])
			if err != nil {
				continue
			}

			if above, err := aboveMaxDepth(acc.setts.clock, time.Unix(int64(tx.Timestamp), 0)); !above || err != nil {
				continue
			}

			tailToPromote = tailTx
			break
		}

		bndl, err := store.PendingTransferToBundle(pendingTransfer)
		if err != nil {
			continue
		}

		// promote as tail was found
		if len(tailToPromote) > 0 {
			promoteTailTxHash, err := promote(tailToPromote)
			if err != nil {
				acc.setts.eventMachine.Emit(ErrorEvent{Error: errors.Wrap(err, "unable to promote"), Type: ErrorPromoteTransfer}, EventError)
				continue
			}
			acc.setts.eventMachine.Emit(PromotionReattachmentEvent{
				BundleHash:          bndl[0].Bundle,
				PromotionTailTxHash: promoteTailTxHash,
				OriginTailTxHash:    key,
			}, EventPromotion)
			continue
		}

		// reattach
		reattachTailTxHash, err := reattach(bndl)
		if err != nil {
			acc.setts.eventMachine.Emit(ErrorEvent{Error: errors.Wrap(err, "unable to reattach bundle"), Type: ErrorReattachTransfer}, EventError)
			continue
		}
		acc.setts.eventMachine.Emit(PromotionReattachmentEvent{
			BundleHash:             bndl[0].Bundle,
			OriginTailTxHash:       key,
			ReattachmentTailTxHash: reattachTailTxHash,
		}, EventReattachment)
		if !storeTailTxHash(key, reattachTailTxHash, "unable to store reattachment tx tail hash", ErrorReattachTransfer) {
			continue
		}
		promoteTailTxHash, err := promote(reattachTailTxHash)
		if err != nil {
			acc.setts.eventMachine.Emit(ErrorEvent{Error: errors.Wrap(err, "unable to promote reattached bundle"), Type: ErrorPromoteTransfer}, EventError)
			continue
		}
		acc.setts.eventMachine.Emit(PromotionReattachmentEvent{
			BundleHash:          bndl[0].Bundle,
			OriginTailTxHash:    key,
			PromotionTailTxHash: promoteTailTxHash,
		}, EventPromotion)
	}
}

// PerTailFilter filters receiving/received bundles by the bundle's tail transaction hash.
// Optionally takes in a bool flag indicating whether the first pass of the event filter
// should not emit any events.
func NewPerTailReceiveEventFilter(skipFirst ...bool) ReceiveEventFilter {
	var skipFirstEmitting bool
	if len(skipFirst) > 0 && skipFirst[0] {
		skipFirstEmitting = true
	}
	receivedFilter := make(map[string]struct{})
	receivingFilter := make(map[string]struct{})

	return func(em EventMachine, bndls bundle.Bundles, ownDepAddrs StringSet, ownSpentAddrs StringSet) {
		events := []ReceiveEventTuple{}

		receivingBundles := make(map[string]bundle.Bundle)
		receivedBundles := make(map[string]bundle.Bundle)

		// filter out transfers to own remainder addresses or where
		// a deposit is an input address (a spend from our own address)
		for _, bndl := range bndls {
			if err := bundle.ValidBundle(bndl); err != nil {
				continue
			}

			isSpendFromOwnAddr := false
			isTransferToOwnRemainderAddr := false

			// filter transfers to remainder addresses by checking
			// whether an input address is an own spent address
			for i := range bndl {
				if _, has := ownSpentAddrs[bndl[i].Address]; has && bndl[i].Value < 0 {
					isTransferToOwnRemainderAddr = true
					break
				}
			}
			// filter value transfers where a deposit address is an input
			for i := range bndl {
				if _, has := ownDepAddrs[bndl[i].Address]; has && bndl[i].Value < 0 {
					isSpendFromOwnAddr = true
					break
				}
			}
			if isTransferToOwnRemainderAddr || isSpendFromOwnAddr {
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
			if _, has := receivingFilter[tailTx]; has {
				delete(receivingBundles, tailTx)
				continue
			}
			receivingFilter[tailTx] = struct{}{}
			// determine whether the bundle is a value transfer.
			if isValueTransfer(bndl) {
				events = append(events, ReceiveEventTuple{EventReceivingDeposit, bndl})
				continue
			}
			events = append(events, ReceiveEventTuple{EventReceivedMessage, bndl})
		}

		for tailTx, bndl := range receivedBundles {
			if _, has := receivedFilter[tailTx]; has {
				delete(receivedBundles, tailTx)
				continue
			}
			receivedFilter[tailTx] = struct{}{}
			if isValueTransfer(bndl) {
				events = append(events, ReceiveEventTuple{EventReceivedDeposit, bndl})
				continue
			}
			events = append(events, ReceiveEventTuple{EventReceivedMessage, bndl})
		}

		// skip first emitting of events as multiple restarts of the same account
		// would yield the same events.
		if skipFirstEmitting {
			skipFirstEmitting = false
			return
		}

		for _, event := range events {
			em.Emit(event.Bundle, event.Event)
		}
	}
}
