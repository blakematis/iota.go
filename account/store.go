package account

import (
	"fmt"
	"github.com/iotaledger/iota.go/bundle"
	"github.com/iotaledger/iota.go/consts"
	"github.com/iotaledger/iota.go/transaction"
	. "github.com/iotaledger/iota.go/trinary"
	"github.com/pkg/errors"
	"time"
)

func newaccountstate() *AccountState {
	return &AccountState{
		DepositRequests:  make(map[uint64]*DepositRequest, 0),
		PendingTransfers: make(map[string]*pendingtransfer, 0),
	}
}

type DepositConditions struct {
	DepositRequest
	Address Hash `json:"address"`
}

type DepositConditionField string

const (
	ConditionExpires  = "t"
	ConditionMultiUse = "m"
	ConditionAmount   = "am"
)

func (dc *DepositConditions) URL() string {
	return fmt.Sprintf("iota://%s/?t=%d&m=%v&am=%d", dc.Address, dc.TimeoutOn.Unix(), dc.MultiUse, dc.ExpectedAmount)
}

type DepositRequest struct {
	// the timeout after this deposit address becomes invalid (creation+timeout)
	TimeoutOn *time.Time `json:"timeout_on"`
	// whether to expect multiple deposits to this address
	// in the given timeout.
	// if this flag is false, the deposit address is considered
	// in the input selection as soon as one deposit is available
	// (if the expected amount is set and also fulfilled)
	MultiUse bool `json:"multi_use"`
	// the expected amount which gets deposited.
	// if the timeout is hit, the address is automatically
	// considered in the input selection.
	ExpectedAmount *uint64 `json:"expected_amount"`
}

type AccountState struct {
	KeyIndex         uint64                      `json:"key_index"`
	DepositRequests  map[uint64]*DepositRequest  `json:"deposit_requests"`
	PendingTransfers map[string]*pendingtransfer `json:"pending_transfers"`
}

func (state *AccountState) IsNew() bool {
	return len(state.DepositRequests) == 0 && len(state.PendingTransfers) == 0
}

type pendingtransfer struct {
	Bundle []Trits `json:"bundle"`
	Tails  Hashes  `json:"tails"`
}

var ErrAccountNotFound = errors.New("account not found")
var ErrPendingTransferNotFound = errors.New("pending transfer not found")
var ErrDepositRequestNotFound = errors.New("deposit request not found")

type Store interface {
	LoadAccount(id string) (*AccountState, error)
	RemoveAccount(id string) error
	ReadIndex(id string) (uint64, error)
	WriteIndex(id string, index uint64) error
	AddDepositRequest(id string, index uint64, depositRequest *DepositRequest) error
	RemoveDepositRequest(id string, index uint64) error
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
