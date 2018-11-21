package account

import (
	"crypto/md5"
	"fmt"
	"github.com/iotaledger/iota.go/address"
	"github.com/iotaledger/iota.go/api"
	"github.com/iotaledger/iota.go/bundle"
	"github.com/iotaledger/iota.go/consts"
	. "github.com/iotaledger/iota.go/trinary"
	"math"
)

func newaccountstate() *AccountState {
	return &AccountState{
		UsedAddresses:    make([]int64, 0),
		PendingTransfers: make([]pendingtransfer, 0),
	}
}

type AccountState struct {
	UsedAddresses    []int64           `json:"used_addresses"`
	PendingTransfers []pendingtransfer `json:"pending_transfers"`
}

func (state *AccountState) DepositAddresses() []uint64 {
	indices := []uint64{}
	for _, index := range state.UsedAddresses {
		if index > 0 {
			continue
		}
		indices = append(indices, uint64(math.Abs(float64(index))))
	}
	return indices
}

func (state *AccountState) SpentAddresses() []uint64 {
	indices := []uint64{}
	for _, index := range state.UsedAddresses {
		if index < 0 {
			continue
		}
		indices = append(indices, uint64(index))
	}
	return indices
}

type pendingtransfer struct {
	bundle []Trits
	tails  Hashes
}

type InputSelectionStrategyFunc func(indices []uint64) []uint64

func NewAccount(seed Trytes, storage Storage, api *api.API) (*Account, error) {
	acc := &Account{
		id:              fmt.Sprintf("%x", md5.Sum([]byte(seed))),
		seed:            seed,
		sendChan:        make(chan sendmsg),
		sendBackChan:    make(chan backmsg),
		receiveChan:     make(chan sendmsg),
		newDepAddrChan:  make(chan struct{}),
		lastDepAddrChan: make(chan struct{}),
		balanceReqChan:  make(chan struct{}),
		api:             api,
		storage:         storage,
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
	id              string
	seed            Trytes
	sendChan        chan sendmsg
	sendBackChan    chan backmsg
	receiveChan     chan sendmsg
	newDepAddrChan  chan struct{}
	lastDepAddrChan chan struct{}
	balanceReqChan  chan struct{}
	newReqChan      chan struct{}
	api             *api.API
	storage         Storage
	lastKeyIndex    uint64
	inputselection  InputSelectionStrategyFunc
}

func (a *Account) Send(target Hash, amount uint64) (bundle.Bundle, error) {
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

func (a *Account) sendError(err error) {
	a.sendBackChan <- backmsg{err: err}
}

func (a *Account) run() error {
	state, err := a.storage.LoadAccount(a.id)
	if err != nil {
		return err
	}
	if len(state.UsedAddresses) > 0 {
		sliceIndex := len(state.UsedAddresses) - 1
		a.lastKeyIndex = uint64(math.Abs(float64(state.UsedAddresses[sliceIndex])))
	}
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
				isNew := len(state.UsedAddresses) == 0
				a.sendBackChan <- backmsg{item: isNew}
			}
		}
	}()
	return nil
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
	}

	transfers := bundle.Transfers{
		{
			Value:   req.amount,
			Address: req.target,
		},
	}
	opts := api.PrepareTransfersOptions{
		Inputs:           inputs,
		RemainderAddress: remainderAddress,
		Security:         consts.SecurityLevelMedium,
	}

	bundleTrytes, err := a.api.PrepareTransfers(a.seed, transfers, opts)
	if err != nil {
		a.sendError(err)
		return
	}

	// add the new transfer to the db
	if err := a.storage.AddPendingTransfers(a.id, bundleTrytes); err != nil {
		a.sendError(err)
		return
	}

	// mark the used addresses as spent
	if inputs != nil {
		for _, input := range inputs {
			if err := a.storage.MarkSpentAddress(a.id, input.KeyIndex); err != nil {
				a.sendError(err)
				return
			}
		}
	}

	bndl, err := a.api.SendTrytes(bundleTrytes, 3, 14)
	if err != nil {
		a.sendError(err)
		return
	}

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
		addr, err := address.GenerateAddress(a.seed, index, consts.SecurityLevelMedium)
		if err != nil {
			return 0, nil, err
		}
		addrsToQuery = append(addrsToQuery, addr)
		input := api.Input{
			Address:  addr,
			KeyIndex: index,
			Security: consts.SecurityLevelMedium,
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
	addresses := make(Hashes, len(state.DepositAddresses()))
	for i, index := range state.DepositAddresses() {
		addr, err := address.GenerateAddress(a.seed, index, consts.SecurityLevelMedium, false)
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
	nextIndex := a.lastKeyIndex + 1
	depositAddr, err := address.GenerateAddress(a.seed, nextIndex, consts.SecurityLevelMedium)
	if err != nil {
		return "", err
	}
	if err := a.storage.MarkDepositAddress(a.id, nextIndex); err != nil {
		return "", err
	}
	a.lastKeyIndex = nextIndex
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
	depositAddr, err := address.GenerateAddress(a.seed, index, consts.SecurityLevelMedium)
	if err != nil {
		return "", err
	}
	return depositAddr, nil
}

func (a *Account) hasIncomingTransfer(index uint64) (bool, error) {
	addr, err := address.GenerateAddress(a.seed, index, consts.SecurityLevelMedium)
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

func (a *Account) incomingTransfers() {
	state, _ := a.storage.LoadAccount(a.id)
	addresses := make(Hashes, len(state.DepositAddresses()))
	for i, index := range state.DepositAddresses() {
		addr, err := address.GenerateAddress(a.seed, index, consts.SecurityLevelMedium, false)
		if err != nil {
			// TODO: do something
			return
		}
		addresses[i] = addr
	}
	txs, err := a.api.FindTransactionObjects(api.FindTransactionsQuery{Addresses: addresses})
	if err != nil {
		// TODO: do something
		return
	}
	hashes := make(Hashes, len(txs))
	for _, tx := range txs {
		hashes = append(hashes, tx.Hash)
	}

	states, err := a.api.GetLatestInclusion(hashes)
	_ = err
	_ = states
}
