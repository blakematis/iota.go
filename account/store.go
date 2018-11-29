package account

import (
	"github.com/iotaledger/iota.go/bundle"
	"github.com/iotaledger/iota.go/consts"
	"github.com/iotaledger/iota.go/transaction"
	. "github.com/iotaledger/iota.go/trinary"
	"github.com/pkg/errors"
	"math"
)

func newaccountstate() *AccountState {
	return &AccountState{
		UsedAddresses:    make([]int64, 0),
		PendingTransfers: make(map[string]*pendingtransfer, 0),
	}
}

type AccountState struct {
	UsedAddresses    []int64                     `json:"used_addresses"`
	PendingTransfers map[string]*pendingtransfer `json:"pending_transfers"`
	lastKeyIndex     uint64
}

func (state *AccountState) IsNew() bool {
	return len(state.UsedAddresses) == 0 && len(state.PendingTransfers) == 0
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
	Bundle []Trits `json:"bundle"`
	Tails  Hashes  `json:"tails"`
}

var ErrAddrIndexNotFound = errors.New("address index not found")
var ErrAccountNotFound = errors.New("account not found")
var ErrPendingTransferNotFound = errors.New("pending transfer not found")

type Store interface {
	LoadAccount(id string) (*AccountState, error)
	RemoveAccount(id string) error
	MarkDepositAddresses(id string, indices ...uint64) error
	AddPendingTransfer(id string, tailTx Hash, bundleTrytes []Trytes, indices ...uint64) error
	RemovePendingTransfer(id string, tailHash Hash) error
	AddTailHash(id string, tailHash Hash, newTailTxHash Hash) error
	GetPendingTransfers(id string) (Hashes, bundle.Bundles, error)
}

func trytesToPendingTransfer(trytes []Trytes) pendingtransfer {
	essences := make([]Trits, len(trytes))
	for i := 0; i < len(trytes); i++ {
		txTrits := MustTrytesToTrits(trytes[i])
		essences[i] = txTrits[consts.AddressTrinaryOffset:consts.BundleTrinaryOffset]
	}
	return pendingtransfer{Bundle: essences, Tails: Hashes{}}
}

func essenceToBundle(pt *pendingtransfer) (bundle.Bundle, error) {
	bndl := make(bundle.Bundle, len(pt.Bundle))
	in := 0
	for i := 0; i < len(bndl); i++ {
		essenceTrits := pt.Bundle[i]
		// add empty trits for fields after the last index
		emptyTxSuffix := PadTrits(Trits{}, consts.TransactionTrinarySize-consts.BundleTrinaryOffset)
		txTrits := append(essenceTrits, emptyTxSuffix...)
		emptySignFrag := PadTrits(Trits{}, consts.SignatureMessageFragmentTrinarySize)
		txTrits = append(emptySignFrag, txTrits...)
		tx, err := transaction.ParseTransaction(txTrits, true)
		if err != nil {
			return nil, err
		}
		bndl[in] = *tx
		in++
	}
	b, err := bundle.Finalize(bndl)
	if err != nil {
		panic(err)
	}
	return b, nil
}
