package store

import (
	"encoding/gob"
	"github.com/iotaledger/iota.go/account/deposit"
	"github.com/iotaledger/iota.go/bundle"
	"github.com/iotaledger/iota.go/consts"
	"github.com/iotaledger/iota.go/guards"
	"github.com/iotaledger/iota.go/transaction"
	. "github.com/iotaledger/iota.go/trinary"
	"github.com/pkg/errors"
)

func init() {
	gob.Register(AccountState{})
}

func newaccountstate() *AccountState {
	return &AccountState{
		DepositRequests:  make(map[uint64]*StoredDepositRequest, 0),
		PendingTransfers: make(map[string]*PendingTransfer, 0),
	}
}

// AccountState is the underlying representation of the account data.
type AccountState struct {
	KeyIndex         uint64                           `json:"key_index" bson:"key_index"`
	DepositRequests  map[uint64]*StoredDepositRequest `json:"deposit_requests" bson:"deposit_requests"`
	PendingTransfers map[string]*PendingTransfer      `json:"pending_transfers" bson:"pending_transfers"`
}

func (state *AccountState) IsNew() bool {
	return len(state.DepositRequests) == 0 && len(state.PendingTransfers) == 0
}

// PendingTransfer defines a pending transfer in the store which is made up of the bundle's
// essence trytes and tail hashes of reattachments.
type PendingTransfer struct {
	Bundle []Trytes `json:"bundle" bson:"bundle"`
	Tails  Hashes   `json:"tails" bson:"tails"`
}

// StoredDepositRequest defines a stored deposit request.
// It differs from the normal request only in having an additional field to hold the security level
// used to generate the deposit address.
type StoredDepositRequest struct {
	deposit.Request
	SecurityLevel consts.SecurityLevel `json:"security_level" bson:"security_level"`
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
	AddDepositRequest(id string, index uint64, depositRequest *StoredDepositRequest) error
	RemoveDepositRequest(id string, index uint64) error
	GetDepositRequests(id string) (map[uint64]*StoredDepositRequest, error)
	AddPendingTransfer(id string, tailTx Hash, bundleTrytes []Trytes, indices ...uint64) error
	RemovePendingTransfer(id string, tailHash Hash) error
	AddTailHash(id string, tailHash Hash, newTailTxHash Hash) error
	GetPendingTransfers(id string) (map[string]*PendingTransfer, error)
}

// TrytesToPendingTransfer converts the given trytes to its essence trits.
func TrytesToPendingTransfer(trytes []Trytes) PendingTransfer {
	essences := make([]Trytes, len(trytes))
	for i := 0; i < len(trytes); i++ {
		// if the transaction has a non empty signature message fragment, we store it in the store
		storeSigMsgFrag := !guards.IsEmptyTrytes(trytes[i][:consts.AddressTrinaryOffset/3])
		if storeSigMsgFrag {
			essences[i] = trytes[i][:consts.BundleTrinaryOffset/3]
		} else {
			essences[i] = trytes[i][consts.AddressTrinaryOffset/3 : consts.BundleTrinaryOffset/3]
		}
	}
	return PendingTransfer{Bundle: essences, Tails: Hashes{}}
}

// PendingTransferToBundle converts bundle essences to a (incomplete) bundle.
func PendingTransferToBundle(pt *PendingTransfer) (bundle.Bundle, error) {
	bndl := make(bundle.Bundle, len(pt.Bundle))
	in := 0
	for i := 0; i < len(bndl); i++ {
		essenceTrytes := pt.Bundle[i]
		// add empty trits for fields after the last index
		txTrytes := essenceTrytes + Pad("", (consts.TransactionTrinarySize-consts.BundleTrinaryOffset)/3)
		// add an empty signature message fragment if non was stored
		if len(txTrytes) != consts.TransactionTrinarySize/3 {
			txTrytes = Pad("", consts.SignatureMessageFragmentTrinarySize/3) + txTrytes
		}
		tx, err := transaction.ParseTransaction(MustTrytesToTrits(txTrytes), true)
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
