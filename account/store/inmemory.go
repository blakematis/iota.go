package store

import (
	"encoding/json"
	"github.com/iotaledger/iota.go/account/deposit"
	"github.com/iotaledger/iota.go/bundle"
	"github.com/iotaledger/iota.go/trinary"
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

func (mem *InMemoryStore) Dump() []byte {
	mem.muAccs.Lock()
	defer mem.muAccs.Unlock()
	dump, err := json.MarshalIndent(mem.accs, "", "   ")
	if err != nil {
		panic(err)
	}
	return dump
}
func (mem *InMemoryStore) LoadAccount(id string) (*AccountState, error) {
	mem.muAccs.Lock()
	defer mem.muAccs.Unlock()
	state, ok := mem.accs[id]
	if !ok {
		mem.accs[id] = newaccountstate()
		return mem.accs[id], nil
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

func (mem *InMemoryStore) ReadIndex(id string) (uint64, error) {
	mem.muAccs.Lock()
	defer mem.muAccs.Unlock()
	state, ok := mem.accs[id]
	if !ok {
		return 0, ErrAccountNotFound
	}
	return state.KeyIndex, nil
}

func (mem *InMemoryStore) WriteIndex(id string, index uint64) (error) {
	mem.muAccs.Lock()
	defer mem.muAccs.Unlock()
	state, ok := mem.accs[id]
	if !ok {
		return ErrAccountNotFound
	}
	state.KeyIndex = index
	return nil
}

func (mem *InMemoryStore) AddDepositRequest(id string, index uint64, depositRequest *deposit.Request) error {
	mem.muAccs.Lock()
	defer mem.muAccs.Unlock()
	state, ok := mem.accs[id]
	if !ok {
		return ErrAccountNotFound
	}
	state.DepositRequests[index] = depositRequest
	return nil
}

func (mem *InMemoryStore) RemoveDepositRequest(id string, index uint64) error {
	mem.muAccs.Lock()
	defer mem.muAccs.Unlock()
	state, ok := mem.accs[id]
	if !ok {
		return ErrAccountNotFound
	}
	_, ok = state.DepositRequests[index]
	if !ok {
		return ErrDepositRequestNotFound
	}
	delete(state.DepositRequests, index)
	return nil
}

func (mem *InMemoryStore) AddPendingTransfer(id string, tailTx trinary.Hash, bundleTrytes []trinary.Trytes, indices ...uint64) error {
	mem.muAccs.Lock()
	defer mem.muAccs.Unlock()
	state, ok := mem.accs[id]
	if !ok {
		return ErrAccountNotFound
	}

	// remove used deposit actions
	for _, index := range indices {
		delete(state.DepositRequests, index)
	}

	pendingTransfer := TrytesToPendingTransfer(bundleTrytes)
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
		bndl, err := PendingTransferToBundle(pendingTransfer)
		if err != nil {
			return nil, nil, err
		}
		bundles[i] = bndl
		tailTxs[i] = tailTx
		i++
	}
	return tailTxs, bundles, nil
}
