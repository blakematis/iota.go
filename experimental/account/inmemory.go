package account

import (
	"github.com/iotaledger/iota.go/bundle"
	"github.com/iotaledger/iota.go/trinary"
	"math"
	"sync"
)

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		accs: map[string]*AccountState{},
	}
}

type InMemoryStore struct {
	muAccs sync.Mutex
	accs   map[string]*AccountState
}

func (mem *InMemoryStore) Clear() {
	mem.muAccs.Lock()
	defer mem.muAccs.Unlock()
	mem.accs = map[string]*AccountState{}
}

func (mem *InMemoryStore) LoadAccount(id string) (*AccountState, error) {
	mem.muAccs.Lock()
	defer mem.muAccs.Unlock()
	state, ok := mem.accs[id]
	if !ok {
		mem.accs[id] = newaccountstate()
		return mem.accs[id], nil
	}
	if len(state.UsedAddresses) > 0 {
		sliceIndex := len(state.UsedAddresses) - 1
		state.lastKeyIndex = uint64(math.Abs(float64(state.UsedAddresses[sliceIndex])))
	}
	return state, nil
}

func (mem *InMemoryStore) RemoveAccount(id string) error {
	mem.muAccs.Lock()
	defer mem.muAccs.Unlock()
	_, ok := mem.accs[id]
	if !ok {
		return ErrAccountNotFound
	}
	delete(mem.accs, id)
	return nil
}

func (mem *InMemoryStore) MarkSpentAddresses(id string, indices ...uint64) error {
	mem.muAccs.Lock()
	defer mem.muAccs.Unlock()
	state, ok := mem.accs[id]
	if !ok {
		return ErrAccountNotFound
	}
	prevState := make([]int64, len(state.UsedAddresses))
	copy(prevState, state.UsedAddresses)
	for _, index := range indices {
		found := false
		for i, usedIndex := range state.UsedAddresses {
			usedIndexU := uint64(math.Abs(float64(usedIndex)))
			if usedIndexU == index {
				state.UsedAddresses[i] = int64(usedIndexU)
				found = true
				break
			}
		}
		if !found {
			state.UsedAddresses = prevState
			return ErrAddrIndexNotFound
		}
	}
	return nil
}

func (mem *InMemoryStore) MarkDepositAddresses(id string, indices ...uint64) error {
	mem.muAccs.Lock()
	defer mem.muAccs.Unlock()
	state, ok := mem.accs[id]
	if !ok {
		return ErrAccountNotFound
	}
	for _, index := range indices {
		state.UsedAddresses = append(state.UsedAddresses, -int64(index))
	}
	return nil
}

func (mem *InMemoryStore) AddPendingTransfer(id string, bundleTrytes []trinary.Trytes) error {
	mem.muAccs.Lock()
	defer mem.muAccs.Unlock()
	state, ok := mem.accs[id]
	if !ok {
		return ErrAccountNotFound
	}
	pendingTransfer := trytesToPendingTransfer(bundleTrytes)
	state.PendingTransfers = append(state.PendingTransfers, pendingTransfer)
	return nil
}

func (mem *InMemoryStore) RemovePendingTransfer(id string, bundleHash trinary.Hash) error {
	mem.muAccs.Lock()
	defer mem.muAccs.Unlock()
	state, ok := mem.accs[id]
	if !ok {
		return ErrAccountNotFound
	}
	_, index, err := bundleIndexByHash(state, bundleHash)
	if err != nil {
		return err
	}
	state.PendingTransfers = append(state.PendingTransfers[:index], state.PendingTransfers[index+1:]...)
	return nil
}

func (mem *InMemoryStore) AddTailHash(id string, bundleHash trinary.Hash, newTailTxHash trinary.Hash) error {
	mem.muAccs.Lock()
	defer mem.muAccs.Unlock()
	state, ok := mem.accs[id]
	if !ok {
		return ErrAccountNotFound
	}
	_, index, err := bundleIndexByHash(state, bundleHash)
	if err != nil {
		return err
	}
	tails := append(state.PendingTransfers[index].Tails, newTailTxHash)
	state.PendingTransfers[index].Tails = tails
	return nil
}

func (mem *InMemoryStore) GetPendingTransfer(id string, bundleHash trinary.Hash) (bundle.Bundle, error) {
	mem.muAccs.Lock()
	defer mem.muAccs.Unlock()
	state, ok := mem.accs[id]
	if !ok {
		return nil, ErrAccountNotFound
	}
	b, _, err := bundleIndexByHash(state, bundleHash)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (mem *InMemoryStore) GetPendingTransfers(id string) (bundle.Bundles, error) {
	mem.muAccs.Lock()
	defer mem.muAccs.Unlock()
	state, ok := mem.accs[id]
	if !ok {
		return nil, ErrAccountNotFound
	}
	bundles := make(bundle.Bundles, len(state.PendingTransfers))
	for i, pendingTransfer := range state.PendingTransfers {
		bndl, err := essenceToBundle(&pendingTransfer)
		if err != nil {
			return nil, err
		}
		bundles[i] = bndl
	}
	return bundles, nil
}
