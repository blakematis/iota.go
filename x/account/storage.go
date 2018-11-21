package account

import (
	"encoding/json"
	"github.com/dgraph-io/badger"
	"github.com/iotaledger/iota.go/bundle"
	"github.com/iotaledger/iota.go/consts"
	"github.com/iotaledger/iota.go/transaction"
	. "github.com/iotaledger/iota.go/trinary"
	"github.com/pkg/errors"
)

var ErrAccountNotFound = errors.New("account not found")
var ErrPendingTransferNotFound = errors.New("pending transfer not found")

type Storage interface {
	LoadAccount(id string) (*AccountState, error)
	RemoveAccount(id string) error
	MarkSpentAddress(id string, index uint64) error
	MarkDepositAddress(id string, index uint64) error
	AddPendingTransfers(id string, bundleTrytes []Trytes) error
	RemovePendingTransfers(id string, bundleHash Hash) error
	AddReattachmentHash(id string, bundleHash Hash, newTailTxHash Hash) error
	GetPendingTransfer(id string, bundleHash Hash) (bundle.Bundle, error)
	GetPendingTransfers(id string) (bundle.Bundles, error)
}

func NewBadgerStorage(dir string) (*BadgerStorage, error) {
	store := &BadgerStorage{dir: dir}
	if err := store.init(); err != nil {
		return nil, err
	}
	return store, nil
}

type BadgerStorage struct {
	db  *badger.DB
	dir string
}

func (b *BadgerStorage) init() error {
	opts := badger.DefaultOptions
	opts.Dir = b.dir
	opts.ValueDir = b.dir
	var err error
	b.db, err = badger.Open(opts)
	return err
}

type statemutationfunc func(state *AccountState) error

func (b *BadgerStorage) mutate(id string, mutFunc statemutationfunc) error {
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

func (b *BadgerStorage) read(id string, readFunc statereadfunc) error {
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

func (b *BadgerStorage) LoadAccount(id string) (*AccountState, error) {
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

func (b *BadgerStorage) RemoveAccount(id string) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(id))
	})
}

func (b *BadgerStorage) MarkSpentAddress(id string, index uint64) error {
	return b.mutate(id, func(state *AccountState) error {
		state.UsedAddresses = append(state.UsedAddresses, int64(index))
		return nil
	})
}

func (b *BadgerStorage) MarkDepositAddress(id string, index uint64) error {
	return b.mutate(id, func(state *AccountState) error {
		state.UsedAddresses = append(state.UsedAddresses, -int64(index))
		return nil
	})
}

func (b *BadgerStorage) AddPendingTransfers(id string, bundleTrytes []Trytes) error {
	// essence: value, timestamp, current index, last index, obsolete tag
	return b.mutate(id, func(state *AccountState) error {
		essences := make([]Trits, len(bundleTrytes))
		for i := 0; i < len(bundleTrytes); i++ {
			txTrits := MustTrytesToTrits(bundleTrytes[i])
			essences[i] = txTrits[consts.AddressTrinaryOffset:consts.BundleTrinaryOffset]
		}
		transfer := pendingtransfer{bundle: essences, tails: Hashes{}}
		state.PendingTransfers = append(state.PendingTransfers, transfer)
		return nil
	})
}

func essenceToBundle(pt *pendingtransfer) (bundle.Bundle, error) {
	bndl := make(bundle.Bundle, len(pt.bundle))
	for i, essenceTrits := range pt.bundle {
		txTrits := PadTrits(essenceTrits, consts.TransactionTrinarySize)
		tx, err := transaction.ParseTransaction(txTrits, true)
		if err != nil {
			return nil, err
		}
		bndl[i] = *tx
	}
	return bundle.Finalize(bndl)
}

func bundleIndexByHash(state *AccountState, bundleHash Hash) (bundle.Bundle, int, error) {
	index := -1
	var b bundle.Bundle
	for i, pendingTransfer := range state.PendingTransfers {
		finBndl, err := essenceToBundle(&pendingTransfer)
		if err != nil {
			return nil, 0, err
		}
		if finBndl[0].Bundle != bundleHash {
			continue
		}
		b = finBndl
		index = i
		break
	}
	if index == -1 {
		return nil, -1, ErrPendingTransferNotFound
	}
	return b, index, nil
}

func (b *BadgerStorage) RemovePendingTransfers(id string, bundleHash Hash) error {
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

func (b *BadgerStorage) AddReattachmentHash(id string, bundleHash Hash, newTailTxHash Hash) error {
	return b.mutate(id, func(state *AccountState) error {
		_, _, err := bundleIndexByHash(state, bundleHash)
		if err != nil {
			return err
		}
		tails := append(state.PendingTransfers[0].tails, newTailTxHash)
		state.PendingTransfers[0].tails = tails
		return nil
	})
}

func (b *BadgerStorage) GetPendingTransfer(id string, bundleHash Hash) (bundle.Bundle, error) {
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

func (b *BadgerStorage) GetPendingTransfers(id string) (bundle.Bundles, error) {
	var bundles bundle.Bundles
	if err := b.read(id, func(state *AccountState) error {
		bundles := make(bundle.Bundles, len(state.PendingTransfers))
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
