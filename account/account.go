package account

import (
	"crypto/sha256"
	"fmt"
	"github.com/iotaledger/iota.go/account/deposit"
	"github.com/iotaledger/iota.go/account/event"
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
	"time"
)

// Account is a thread-safe object encapsulating address management, input selection, promotion and reattachments.
type Account interface {
	// ID returns the account's identifier.
	ID() string
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
	actionUpdateComponents
	actionShutdown
	actionUncleanShutdown
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

func (acc *account) ID() string {
	return acc.id
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
	var await bool
	if len(dontAwaitBackgroundTasks) > 0 && dontAwaitBackgroundTasks[0] {
		await = true
	}
	acc.request <- actionrequest{Action: actionShutdown, Request: await}
	return (<-acc.sendBackChan).err
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

// induces an unclean shutdown. this is mainly used to shutdown the account
// in case an inner goroutine can't commence its work and thereby hinders
// the account to function properly.
func (acc *account) executeUncleanShutdown() {
	acc.request <- actionrequest{Action: actionUncleanShutdown}
}

func (acc *account) sendError(err error) {
	acc.sendBackChan <- actionresponse{err: err}
}

func (acc *account) cleanup() {
	close(acc.exit)
}

func (acc *account) startPlugins() error {
	for _, p := range acc.setts.plugins {
		if err := p.Start(acc); err != nil {
			return err
		}
	}
	return nil
}

func (acc *account) shutdownPlugins() error {
	for _, p := range acc.setts.plugins {
		if err := p.Shutdown(); err != nil {
			return err
		}
	}
	return nil
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

		// start up plugins
		if err := acc.startPlugins(); err != nil {
			acc.setts.eventMachine.Emit(err, event.EventError)
			if err := acc.shutdownPlugins(); err != nil {
				acc.setts.eventMachine.Emit(err, event.EventError)
			}
			acc.cleanup()
		}

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
					addr, err := acc.addrFunc(req.Request.(*deposit.Request))
					acc.sendBackChan <- actionresponse{item: addr, err: err}

				case actionUsableBalance:
					balance, err := acc.usableBalance()
					acc.sendBackChan <- actionresponse{item: balance, err: err}

				case actionTotalBalance:
					balance, err := acc.totalBalance()
					acc.sendBackChan <- actionresponse{item: balance, err: err}

				case actionIsNew:
					state, err := acc.setts.store.LoadAccount(acc.id)
					acc.sendBackChan <- actionresponse{item: state.IsNew(), err: err}

				case actionUpdateComponents:

					// await all ongoing plugins to terminate
					if err := acc.shutdownPlugins(); err != nil {
						acc.setts.eventMachine.Emit(err, event.EventError)
						break exit
					}

					// update the settings of the account
					newSetts, ok := req.Request.(*Settings)
					if newSetts != nil && ok {
						// make a copy
						settingsCopy := *newSetts
						acc.setts = &settingsCopy
					}

					// continue polling goroutines
					if err := acc.startPlugins(); err != nil {
						acc.setts.eventMachine.Emit(err, event.EventError)
						break exit
					}
				case actionExecuteSync:
					f, ok := req.Request.(syncfunc)
					if !ok {
						continue
					}
					acc.sendBackChan <- actionresponse{err: f()}
				case actionUncleanShutdown:
					acc.cleanup()
					break exit
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
			// await all plugins to shutdown
			if err := acc.shutdownPlugins(); err != nil {
				acc.setts.eventMachine.Emit(err, event.EventError)
			}
			// TODO: maybe remove select?
			select {
			case acc.sendBackChan <- actionresponse{}:
			default:
			}
			acc.setts.eventMachine.Emit(struct{}{}, event.EventShutdown)
		}
	}()
	return nil
}

// AddrFunc is a function which takes in a deposit request and creates
// an object describing the conditions for the deposit.
// An AddrFunc is not thread-safe.
type AddrFunc func(dep *deposit.Request) (*deposit.Conditions, error)

func (acc *account) newDepositAddressGenerator() (AddrFunc, error) {
	state, err := acc.setts.store.LoadAccount(acc.id)
	if err != nil {
		return nil, err
	}

	seed, err := acc.setts.seedProv.Seed()
	if err != nil {
		return nil, err
	}

	addrGenStopped := make(chan struct{})

	// generating N addresses ahead into the buffer
	go func() {
		defer func() {
			close(addrGenStopped)
		}()
		for index := state.KeyIndex + 1; ; index++ {
			// copy
			secLevl := acc.setts.securityLevel
			addr, err := address.GenerateAddress(seed, index, secLevl, true)
			if err != nil {
				acc.setts.eventMachine.Emit(errors.Wrap(err, "unable to generate address in address gen. function"), event.EventError)
				acc.executeUncleanShutdown()
				return
			}
			if err := acc.setts.store.WriteIndex(acc.id, index); err != nil {
				acc.setts.eventMachine.Emit(errors.Wrapf(err, "unable to store next index (%d) in the store", index), event.EventError)
				acc.executeUncleanShutdown()
				return
			}
			select {
			case acc.addrBuff <- addrindextuple{addr, index, secLevl}:
			case <-acc.exit:
				return
			}
		}
	}()

	return func(req *deposit.Request) (*deposit.Conditions, error) {
		select {
		case <-addrGenStopped:
			return nil, ErrAddrGeneratorStopped
		case tuple := <-acc.addrBuff:
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
				return nil, err
			}
			return &deposit.Conditions{Address: tuple.addr, Request: *req}, nil
		}

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
			depCond, err := acc.addrFunc(&deposit.Request{ExpectedAmount: &remainder})
			if err != nil {
				return nil, errors.Wrap(err, "unable to generate remainder address in send op.")
			}
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

	acc.setts.eventMachine.Emit(bndl, event.EventSendingTransfer)
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
					acc.setts.eventMachine.Emit(errors.Wrap(err, "unable to remove timed out address in input selection"), event.EventError)
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
