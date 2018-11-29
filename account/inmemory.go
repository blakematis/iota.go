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

func (mem *InMemoryStore) AddPendingTransfer(id string, tailTx trinary.Hash, bundleTrytes []trinary.Trytes, indices ...uint64) error {
	mem.muAccs.Lock()
	defer mem.muAccs.Unlock()
	state, ok := mem.accs[id]
	if !ok {
		return ErrAccountNotFound
	}

	// mark spent addresses
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

	pendingTransfer := trytesToPendingTransfer(bundleTrytes)
	pendingTransfer.Tails = append(pendingTransfer.Tails, tailTx)
	state.PendingTransfers[tailTx] = &pendingTransfer
	return nil
}

func (mem *InMemoryStore) RemovePendingTransfer(id string, tailTx trinary.Hash) error {
	mem.muAccs.Lock()
	defer mem.muAccs.Unlock()
	state, ok := mem.accs[id]
	if !ok {
		return ErrAccountNotFound
	}
	if _, ok := state.PendingTransfers[tailTx]; !ok {
		return ErrPendingTransferNotFound
	}
	delete(state.PendingTransfers, tailTx)
	return nil
}

func (mem *InMemoryStore) AddTailHash(id string, tailTx trinary.Hash, newTailTxHash trinary.Hash) error {
	mem.muAccs.Lock()
	defer mem.muAccs.Unlock()
	state, ok := mem.accs[id]
	if !ok {
		return ErrAccountNotFound
	}

	pendingTransfer, ok := state.PendingTransfers[tailTx];
	if !ok {
		return ErrPendingTransferNotFound
	}
	pendingTransfer.Tails = append(pendingTransfer.Tails, newTailTxHash)
	return nil
}

func (mem *InMemoryStore) GetPendingTransfers(id string) (trinary.Hashes, bundle.Bundles, error) {
	mem.muAccs.Lock()
	defer mem.muAccs.Unlock()
	state, ok := mem.accs[id]
	if !ok {
		return nil, nil, ErrAccountNotFound
	}
	bundles := make(bundle.Bundles, len(state.PendingTransfers))
	tailTxs := make(trinary.Hashes, len(state.PendingTransfers))
	i := 0
	for tailTx, pendingTransfer := range state.PendingTransfers {
		bndl, err := essenceToBundle(pendingTransfer)
		if err != nil {
			return nil, nil, err
		}
		bundles[i] = bndl
		tailTxs[i] = tailTx
		i++
	}
	return tailTxs, bundles, nil
}
