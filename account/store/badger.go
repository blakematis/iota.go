package store

import (
	"bytes"
	"encoding/gob"
	"github.com/dgraph-io/badger"
	"github.com/iotaledger/iota.go/account/deposit"
	"github.com/iotaledger/iota.go/bundle"
	. "github.com/iotaledger/iota.go/trinary"
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
		if err != nil {
			return err
		}
		state := newaccountstate()
		dec := gob.NewDecoder(bytes.NewReader(accountBytes))
		if err := dec.Decode(state); err != nil {
			return err
		}
		if err := mutFunc(state); err != nil {
			return err
		}
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(state); err != nil {
			return err
		}
		return txn.Set(key, buf.Bytes())
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
		if err != nil {
			return err
		}
		state = newaccountstate()
		dec := gob.NewDecoder(bytes.NewReader(accountBytes))
		if err := dec.Decode(state); err != nil {
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
		return state, nil
	}
	// if the account is nil, it doesn't exist, lets create it
	state = newaccountstate()
	key := []byte(id)
	if err := b.db.Update(func(txn *badger.Txn) error {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(state); err != nil {
			return err
		}
		return txn.Set(key, buf.Bytes())
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

func (b *BadgerStore) ReadIndex(id string) (uint64, error) {
	var keyIndex uint64
	if err := b.read(id, func(state *AccountState) error {
		keyIndex = state.KeyIndex
		return nil
	}); err != nil {
		return 0, err
	}
	return keyIndex, nil
}

func (b *BadgerStore) WriteIndex(id string, index uint64) (error) {
	return b.mutate(id, func(state *AccountState) error {
		state.KeyIndex = index
		return nil
	})
}

func (b *BadgerStore) AddDepositRequest(id string, index uint64, depositRequest *deposit.Request) error {
	return b.mutate(id, func(state *AccountState) error {
		state.DepositRequests[index] = depositRequest
		return nil
	})
}

func (b *BadgerStore) RemoveDepositRequest(id string, index uint64) error {
	return b.mutate(id, func(state *AccountState) error {
		_, ok := state.DepositRequests[index]
		if !ok {
			return ErrDepositRequestNotFound
		}
		delete(state.DepositRequests, index)
		return nil
	})
}

func (b *BadgerStore) AddPendingTransfer(id string, tailTx Hash, bundleTrytes []Trytes, indices ...uint64) error {
	// essence: value, timestamp, current index, last index, obsolete tag
	return b.mutate(id, func(state *AccountState) error {
		for _, index := range indices {
			delete(state.DepositRequests, index)
		}
		pendingTransfer := TrytesToPendingTransfer(bundleTrytes)
		pendingTransfer.Tails = append(pendingTransfer.Tails, tailTx)
		state.PendingTransfers[tailTx] = &pendingTransfer
		return nil
	})
}

func (b *BadgerStore) RemovePendingTransfer(id string, tailTx Hash) error {
	return b.mutate(id, func(state *AccountState) error {
		if _, ok := state.PendingTransfers[tailTx]; !ok {
			return ErrPendingTransferNotFound
		}
		delete(state.PendingTransfers, tailTx)
		return nil
	})
}

func (b *BadgerStore) AddTailHash(id string, tailTx Hash, newTailTxHash Hash) error {
	return b.mutate(id, func(state *AccountState) error {
		pendingTransfer, ok := state.PendingTransfers[tailTx];
		if !ok {
			return ErrPendingTransferNotFound
		}
		pendingTransfer.Tails = append(pendingTransfer.Tails, newTailTxHash)
		return nil
	})
}

func (b *BadgerStore) GetPendingTransfers(id string) (Hashes, bundle.Bundles, error) {
	var bundles bundle.Bundles
	var tailTxs Hashes
	if err := b.read(id, func(state *AccountState) error {
		bundles = make(bundle.Bundles, len(state.PendingTransfers))
		tailTxs = make(Hashes, len(state.PendingTransfers))
		i := 0
		for tailTx, pendingTransfer := range state.PendingTransfers {
			bndl, err := PendingTransferToBundle(pendingTransfer)
			if err != nil {
				return err
			}
			bundles[i] = bndl
			tailTxs[i] = tailTx
			i++
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}
	return tailTxs, bundles, nil
}
