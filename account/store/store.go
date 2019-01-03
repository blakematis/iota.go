package store

import (
	"encoding/gob"
	"github.com/iotaledger/iota.go/account/deposit"
	"github.com/iotaledger/iota.go/bundle"
	"github.com/iotaledger/iota.go/consts"
	"github.com/iotaledger/iota.go/transaction"
	. "github.com/iotaledger/iota.go/trinary"
	"github.com/pkg/errors"
)

func init() {
	gob.Register(AccountState{})
}

func newaccountstate() *AccountState {
	return &AccountState{
		DepositRequests:  make(map[uint64]*deposit.Request, 0),
		PendingTransfers: make(map[string]*PendingTransfer, 0),
	}
}

// AccountState is the underlying representation of the account data.
type AccountState struct {
	KeyIndex         uint64                      `json:"key_index"`
	DepositRequests  map[uint64]*deposit.Request `json:"deposit_requests"`
	PendingTransfers map[string]*PendingTransfer `json:"pending_transfers"`
}

func (state *AccountState) IsNew() bool {
	return len(state.DepositRequests) == 0 && len(state.PendingTransfers) == 0
}

// PendingTransfer defines a pending transfer in the store which is made up of the bundle's
// essence trits and tail hashes of reattachments.
type PendingTransfer struct {
	Bundle []Trits `json:"bundle"`
	Tails  Hashes  `json:"tails"`
}

// errors produced by the store package.
var (
	ErrAccountNotFound         = errors.New("account not found")
	ErrPendingTransferNotFound = errors.New("pending transfer not found")
	ErrDepositRequestNotFound  = errors.New("deposit request not found")
)

// Store defines a persistence layer which takes care of storing account data.
type Store interface {
	LoadAccount(id string) (*AccountState, error)
	RemoveAccount(id string) error
	ReadIndex(id string) (uint64, error)
	WriteIndex(id string, index uint64) error
	AddDepositRequest(id string, index uint64, depositRequest *deposit.Request) error
	RemoveDepositRequest(id string, index uint64) error
	AddPendingTransfer(id string, tailTx Hash, bundleTrytes []Trytes, indices ...uint64) error
	RemovePendingTransfer(id string, tailHash Hash) error
	AddTailHash(id string, tailHash Hash, newTailTxHash Hash) error
	GetPendingTransfers(id string) (Hashes, bundle.Bundles, error)
}

// TrytesToPendingTransfer converts the given trytes to its essence trits.
func TrytesToPendingTransfer(trytes []Trytes) PendingTransfer {
	essences := make([]Trits, len(trytes))
	for i := 0; i < len(trytes); i++ {
		txTrits := MustTrytesToTrits(trytes[i])
		essences[i] = txTrits[consts.AddressTrinaryOffset:consts.BundleTrinaryOffset]
	}
	return PendingTransfer{Bundle: essences, Tails: Hashes{}}
}

// PendingTransferToBundle converts bundle essences to a (incomplete) bundle.
func PendingTransferToBundle(pt *PendingTransfer) (bundle.Bundle, error) {
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
