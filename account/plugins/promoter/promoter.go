package promoter

import (
	"github.com/iotaledger/iota.go/account"
	"github.com/iotaledger/iota.go/account/event"
	"github.com/iotaledger/iota.go/account/store"
	"github.com/iotaledger/iota.go/api"
	"github.com/iotaledger/iota.go/bundle"
	"github.com/iotaledger/iota.go/transaction"
	. "github.com/iotaledger/iota.go/trinary"
	"github.com/pkg/errors"
	"strings"
	"time"
)

var ErrUnableToPromote = errors.New("unable to promote")
var ErrUnableToReattach = errors.New("unable to reattach")

// own emitted events
const (
	// emitted when a promotion occurred
	EventPromotion event.Event = 200
	// emitted when a reattachment occurred
	EventReattachment event.Event = 201
)

// PromotionReattachmentEvent is emitted when a promotion or reattachment happened.
type PromotionReattachmentEvent struct {
	// The tail tx hash of the first bundle broadcasted to the network.
	OriginTailTxHash Hash `json:"original_tail_tx_hash"`
	// The bundle hash of the promoted/reattached bundle.
	BundleHash Hash `json:"bundle_hash"`
	// The tail tx hash of the promotion transaction.
	PromotionTailTxHash Hash `json:"promotion_tail_tx_hash"`
	// The tail tx hash of the reattached bundle.
	ReattachmentTailTxHash Hash `json:"reattachment_tail_tx_hash"`
}

func NewPromoter(
	api *api.API, store store.Store, eventMachine event.EventMachine, clock account.Clock,
	interval time.Duration, depth uint64, mwm uint64,
) *Promoter {
	if eventMachine == nil {
		eventMachine = &event.DiscardEventMachine{}
	}
	return &Promoter{
		interval: interval, em: eventMachine,
		api: api, store: store, depth: depth, mwm: mwm,
		syncer: make(chan struct{}), shutdown: make(chan struct{}),
	}
}

type Promoter struct {
	interval time.Duration
	api      *api.API
	store    store.Store
	em       event.EventMachine
	clock    account.Clock
	depth    uint64
	mwm      uint64
	acc account.Account
	syncer   chan struct{}
	shutdown chan struct{}
}

func (p *Promoter) Start(acc account.Account) error {
	p.acc = acc
	go func() {
	exit:
		for {
			select {
			case <-time.After(p.interval):
				p.promote()
				select {
				case <-p.shutdown:
					break exit
				default:
				}
				// check for pause signal
			case p.syncer <- struct{}{}:
				// await resume signal
				<-p.syncer
			case <-p.shutdown:
				break exit
			}
		}
		close(p.syncer)
	}()
	return nil
}

// ManuelPoll awaits the current promotion/reattachment task to finish (in case it's ongoing),
// pauses the task, does a manual promotion/reattachment task, resumes the repeated task and then returns.
func (p *Promoter) ManuelPoll() error {
	p.pause()
	defer p.resume()
	p.promote()
	return nil
}

func (p *Promoter) pause() {
	<-p.syncer
}

func (p *Promoter) resume() {
	p.syncer <- struct{}{}
}

func (p *Promoter) Shutdown() error {
	p.shutdown <- struct{}{}
	return nil
}

const approxAboveMaxDepthMinutes = 5

func aboveMaxDepth(clock account.Clock, ts time.Time) (bool, error) {
	currentTime, err := clock.Now()
	if err != nil {
		return false, err
	}
	return currentTime.Sub(ts).Minutes() < approxAboveMaxDepthMinutes, nil
}

const maxDepth = 15
const referenceTooOldMsg = "reference transaction is too old"

var emptySeed = strings.Repeat("9", 81)
var ErrUnpromotableTail = errors.New("tail is unpromoteable")

func (p *Promoter) promote() {
	pendingTransfers, err := p.store.GetPendingTransfers(p.acc.ID())
	if err != nil {
		return
	}
	if len(pendingTransfers) == 0 {
		return
	}

	send := func(preparedBundle []Trytes, tips *api.TransactionsToApprove) (Hash, error) {
		readyBundle, err := p.api.AttachToTangle(tips.TrunkTransaction, tips.BranchTransaction, p.mwm, preparedBundle)
		if err != nil {
			return "", errors.Wrap(err, "performing PoW for promote/reattach cycle bundle failed")
		}
		readyBundle, err = p.api.StoreAndBroadcast(readyBundle)
		if err != nil {
			return "", errors.Wrap(err, "unable to store/broadcast bundle in promote/reattach cycle")
		}
		tailTx, err := transaction.AsTransactionObject(readyBundle[0])
		if err != nil {
			return "", err
		}
		return tailTx.Hash, nil
	}

	promote := func(tailTx Hash) (Hash, error) {
		depth := p.depth
		for {
			tips, err := p.api.GetTransactionsToApprove(depth, tailTx)
			if err != nil {
				if err.Error() == referenceTooOldMsg {
					depth++
					if depth > maxDepth {
						return "", ErrUnpromotableTail
					}
					continue
				}
				return "", err
			}
			pTransfers := bundle.Transfers{bundle.EmptyTransfer}
			preparedBundle, err := p.api.PrepareTransfers(emptySeed, pTransfers, api.PrepareTransfersOptions{})
			if err != nil {
				return "", errors.Wrap(err, "unable to prepare promotion bundle")
			}
			return send(preparedBundle, tips)
		}
	}

	reattach := func(essenceBndl bundle.Bundle) (Hash, error) {
		tips, err := p.api.GetTransactionsToApprove(p.depth)
		if err != nil {
			return "", errors.Wrapf(err, "unable to GTTA for reattachment in promote/reattach cycle (bundle %s)", essenceBndl[0].Bundle)
		}
		essenceTrytes, err := transaction.TransactionsToTrytes(essenceBndl)
		if err != nil {
			return "", err
		}
		return send(essenceTrytes, tips)
	}

	storeTailTxHash := func(key string, tailTxHash string, msg string) bool {
		if err := p.store.AddTailHash(p.acc.ID(), key, tailTxHash); err != nil {
			// might have been removed by polling goroutine
			if err == store.ErrPendingTransferNotFound {
				return true
			}
			p.em.Emit(errors.Wrap(err, msg), event.EventError)
			return false
		}
		return true
	}

	for key, pendingTransfer := range pendingTransfers {
		// search for tail transaction which is consistent and above max depth
		var tailToPromote string
		// go in reverse order to start from the most recent tails
		for i := len(pendingTransfer.Tails) - 1; i >= 0; i-- {
			tailTx := pendingTransfer.Tails[i]
			consistent, _, err := p.api.CheckConsistency(tailTx)
			if err != nil {
				continue
			}

			if !consistent {
				continue
			}

			txTrytes, err := p.api.GetTrytes(tailTx)
			if err != nil {
				continue
			}

			tx, err := transaction.AsTransactionObject(txTrytes[0])
			if err != nil {
				continue
			}

			if above, err := aboveMaxDepth(p.clock, time.Unix(int64(tx.Timestamp), 0)); !above || err != nil {
				continue
			}

			tailToPromote = tailTx
			break
		}

		bndl, err := store.PendingTransferToBundle(pendingTransfer)
		if err != nil {
			continue
		}

		// promote as tail was found
		if len(tailToPromote) > 0 {
			promoteTailTxHash, err := promote(tailToPromote)
			if err != nil {
				p.em.Emit(errors.Wrap(ErrUnableToPromote, err.Error()), event.EventError)
				continue
			}
			p.em.Emit(PromotionReattachmentEvent{
				BundleHash:          bndl[0].Bundle,
				PromotionTailTxHash: promoteTailTxHash,
				OriginTailTxHash:    key,
			}, EventPromotion)
			continue
		}

		// reattach
		reattachTailTxHash, err := reattach(bndl)
		if err != nil {
			p.em.Emit(errors.Wrap(ErrUnableToReattach, err.Error()), event.EventError)
			continue
		}
		p.em.Emit(PromotionReattachmentEvent{
			BundleHash:             bndl[0].Bundle,
			OriginTailTxHash:       key,
			ReattachmentTailTxHash: reattachTailTxHash,
		}, EventReattachment)
		if !storeTailTxHash(key, reattachTailTxHash, "unable to store reattachment tx tail hash") {
			continue
		}
		promoteTailTxHash, err := promote(reattachTailTxHash)
		if err != nil {
			p.em.Emit(errors.Wrap(ErrUnableToPromote, err.Error()), event.EventError)
			continue
		}
		p.em.Emit(PromotionReattachmentEvent{
			BundleHash:          bndl[0].Bundle,
			OriginTailTxHash:    key,
			PromotionTailTxHash: promoteTailTxHash,
		}, EventPromotion)
	}
}
