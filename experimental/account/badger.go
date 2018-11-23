package account

import (
	"encoding/json"
	"github.com/dgraph-io/badger"
	"github.com/iotaledger/iota.go/bundle"
	. "github.com/iotaledger/iota.go/trinary"
	"math"
)

func NewBadgerStore(dir string) (*BadgerStore, error) {
	store := &BadgerStore{dir: dir}
	if err := store.init(); err != nil {
		return nil, err
	}
	return store, nil
}

type BadgerStore struct {
	db  *badger.DB
	dir string
}

func (b *BadgerStore) init() error {
	opts := badger.DefaultOptions
	opts.Dir = b.dir
	opts.ValueDir = b.dir
	var err error
	b.db, err = badger.Open(opts)
	return err
}

type statemutationfunc func(state *AccountState) error

func (b *BadgerStore) mutate(id string, mutFunc statemutationfunc) error {
	key := []byte(id)
	return b.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		accountBytes, err := item.Value()
		state := newaccountstate()
		if err := json.Unmarshal(accountBytes, state); err != nil {
			return err
		}
		if err := mutFunc(state); err != nil {
			return err
		}
		newStateBytes, err := json.Marshal(state)
		if err != nil {
			return err
		}
		return txn.Set(key, newStateBytes)
	})
}

type statereadfunc func(state *AccountState) error

func (b *BadgerStore) read(id string, readFunc statereadfunc) error {
	key := []byte(id)
	if err := b.db.View(func(txn *badger.Txn) error {
		var state *AccountState
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return readFunc(nil)
		}
		accountBytes, err := item.Value()
		state = newaccountstate()
		if err := json.Unmarshal(accountBytes, state); err != nil {
			return err
		}
		return readFunc(state)
	}); err != nil {
		return err
	}
	return nil
}

func (b *BadgerStore) LoadAccount(id string) (*AccountState, error) {
	var state *AccountState
	if err := b.read(id, func(st *AccountState) error {
		state = st
		return nil
	}); err != nil {
		return nil, err
	}
	if state != nil {
		if len(state.UsedAddresses) > 0 {
			sliceIndex := len(state.UsedAddresses) - 1
			state.lastKeyIndex = uint64(math.Abs(float64(state.UsedAddresses[sliceIndex])))
		}
		return state, nil
	}
	// if the account is nil, it doesn't exist, lets create it
	state = newaccountstate()
	key := []byte(id)
	if err := b.db.Update(func(txn *badger.Txn) error {
		newStateBytes, err := json.Marshal(state)
		if err != nil {
			return err
		}
		return txn.Set(key, newStateBytes)
	}); err != nil {
		return nil, err
	}
	return state, nil
}

func (b *BadgerStore) RemoveAccount(id string) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(id))
	})
}

func (b *BadgerStore) MarkSpentAddresses(id string, indices ...uint64) error {
	return b.mutate(id, func(state *AccountState) error {
		// find the deposit addresses corresponding to the given index
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
				return ErrAddrIndexNotFound
			}
		}
		return nil
	})
}

func (b *BadgerStore) MarkDepositAddresses(id string, indices ...uint64) error {
	return b.mutate(id, func(state *AccountState) error {
		for _, index := range indices {
			state.UsedAddresses = append(state.UsedAddresses, -int64(index))
		}
		return nil
	})
}

func (b *BadgerStore) AddPendingTransfer(id string, bundleTrytes []Trytes) error {
	// essence: value, timestamp, current index, last index, obsolete tag
	return b.mutate(id, func(state *AccountState) error {
		pendingTransfer := trytesToPendingTransfer(bundleTrytes)
		state.PendingTransfers = append(state.PendingTransfers, pendingTransfer)
		return nil
	})
}

func (b *BadgerStore) RemovePendingTransfer(id string, bundleHash Hash) error {
	return b.mutate(id, func(state *AccountState) error {
		_, index, err := bundleIndexByHash(state, bundleHash)
		if err != nil {
			return err
		}
		// not found
		if index == -1 {
			return ErrAccountNotFound
		}
		state.PendingTransfers = append(state.PendingTransfers[:index], state.PendingTransfers[index+1:]...)
		return nil
	})
}

func (b *BadgerStore) AddTailHash(id string, bundleHash Hash, newTailTxHash Hash) error {
	return b.mutate(id, func(state *AccountState) error {
		_, i, err := bundleIndexByHash(state, bundleHash)
		if err != nil {
			return err
		}
		tails := append(state.PendingTransfers[i].Tails, newTailTxHash)
		state.PendingTransfers[i].Tails = tails
		return nil
	})
}

func (b *BadgerStore) GetPendingTransfer(id string, bundleHash Hash) (bundle.Bundle, error) {
	var bndl bundle.Bundle
	if err := b.read(id, func(state *AccountState) error {
		b, _, err := bundleIndexByHash(state, bundleHash)
		if err != nil {
			return err
		}
		bndl = b
		return nil
	}); err != nil {
		return nil, err
	}
	return bndl, nil
}

func (b *BadgerStore) GetPendingTransfers(id string) (bundle.Bundles, error) {
	var bundles bundle.Bundles
	if err := b.read(id, func(state *AccountState) error {
		bundles = make(bundle.Bundles, len(state.PendingTransfers))
		for i, pendingTransfer := range state.PendingTransfers {
			bndl, err := essenceToBundle(&pendingTransfer)
			if err != nil {
				return err
			}
			bundles[i] = bndl
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return bundles, nil
}
