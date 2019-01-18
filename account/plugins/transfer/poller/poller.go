package poller

import (
	"github.com/iotaledger/iota.go/account"
	"github.com/iotaledger/iota.go/account/event"
	"github.com/iotaledger/iota.go/account/store"
	"github.com/iotaledger/iota.go/address"
	"github.com/iotaledger/iota.go/api"
	"github.com/iotaledger/iota.go/bundle"
	. "github.com/iotaledger/iota.go/trinary"
	"github.com/pkg/errors"
	"sync"
	"time"
)

// StringSet is a set of strings.
type StringSet map[string]struct{}

// ReceiveEventFilter filters and creates events given the incoming bundles, deposit requests and spent addresses.
// It's the job of the ReceiveEventFilter to emit the appropriate events through the given EventMachine.
type ReceiveEventFilter func(eventMachine event.EventMachine, bndls bundle.Bundles, depAddrs StringSet, spentAddrs StringSet)

const (
	// emitted when a broadcasted transfer got confirmed
	EventTransferConfirmed event.Event = 100
	// emitted when a deposit is being received
	EventReceivingDeposit event.Event = 101
	// emitted when a deposit is confirmed
	EventReceivedDeposit event.Event = 102
	// emitted when a zero value transaction is received
	EventReceivedMessage event.Event = 103
)

func NewTransferPoller(
	api *api.API, store store.Store, eventMachine event.EventMachine,
	seedProv account.SeedProvider, receiveEventFilter ReceiveEventFilter, interval time.Duration,
) *TransferPoller {
	if eventMachine == nil {
		eventMachine = &event.DiscardEventMachine{}
	}
	return &TransferPoller{
		api: api, store: store, interval: interval,
		receiveEventfilter: receiveEventFilter,
		em:                 eventMachine, seedProvider: seedProv,
		shutdown: make(chan struct{}),
		syncer:   make(chan struct{}),
	}
}

type TransferPoller struct {
	api                *api.API
	store              store.Store
	em                 event.EventMachine
	seedProvider       account.SeedProvider
	receiveEventfilter ReceiveEventFilter
	interval           time.Duration
	acc                account.Account
	syncer             chan struct{}
	shutdown           chan struct{}
}

func (tp *TransferPoller) Start(acc account.Account) error {
	tp.acc = acc
	go func() {
	exit:
		for {
			select {
			case <-time.After(tp.interval):
				tp.pollTransfers()
				select {
				case <-tp.shutdown:
					break exit
				default:
				}
				// check for pause signal
			case tp.syncer <- struct{}{}:
				// await resume signal
				<-tp.syncer
			case <-tp.shutdown:
				break exit
			}
		}
		close(tp.syncer)
	}()
	return nil
}

// ManuelPoll awaits the current transfer polling task to finish (in case it's ongoing),
// pauses the task, does a manual transfer polling, resumes the transfer poll task and then returns.
func (tp *TransferPoller) ManuelPoll() error {
	if tp.acc == nil {
		return nil
	}
	// await current polling to finish if any
	if err := tp.pause(); err != nil {
		return err
	}
	defer tp.resume()
	tp.pollTransfers()
	return nil
}

func (tp *TransferPoller) pause() error {
	<-tp.syncer
	return nil
}

func (tp *TransferPoller) resume() error {
	tp.syncer <- struct{}{}
	return nil
}

func (tp *TransferPoller) Shutdown() error {
	tp.shutdown <- struct{}{}
	return nil
}

func (tp *TransferPoller) pollTransfers() {
	pendingTransfers, err := tp.store.GetPendingTransfers(tp.acc.ID())
	if err != nil {
		tp.em.Emit(errors.Wrap(err, "unable to load account state for polling transfers"), event.EventError)
		return
	}

	depositRequests, err := tp.store.GetDepositRequests(tp.acc.ID())
	if err != nil {
		tp.em.Emit(errors.Wrap(err, "unable to load account state for polling transfers"), event.EventError)
		return
	}

	// poll incoming/outgoing in parallel
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		tp.checkOutgoingTransfers(pendingTransfers)
	}()
	go func() {
		defer wg.Done()
		tp.checkIncomingTransfers(depositRequests, pendingTransfers)
	}()
	wg.Wait()
}

func (tp *TransferPoller) checkOutgoingTransfers(pendingTransfers map[string]*store.PendingTransfer) {
	for tailTx, pendingTransfer := range pendingTransfers {
		if len(pendingTransfer.Tails) == 0 {
			continue
		}
		states, err := tp.api.GetLatestInclusion(pendingTransfer.Tails)
		if err != nil {
			tp.em.Emit(errors.Wrapf(err, "unable to check latest inclusion state in outgoing transfers op. (first tail tx of bundle: %s)", tailTx), event.EventError)
			return
		}
		// if any state is true we can remove the transfer as it got confirmed
		for i, state := range states {
			if !state {
				continue
			}
			// fetch bundle to emit it in the event
			bndl, err := tp.api.GetBundle(pendingTransfer.Tails[i])
			if err != nil {
				tp.em.Emit(errors.Wrapf(err, "unable to get bundle in outgoing transfers op. (first tail tx of bundle: %s) of tail %s", tailTx, pendingTransfer.Tails[i]), event.EventError)
				return
			}
			tp.em.Emit(bndl, EventTransferConfirmed)
			if err := tp.store.RemovePendingTransfer(tp.acc.ID(), tailTx); err != nil {
				tp.em.Emit(errors.Wrap(err, "unable to remove confirmed transfer from store in outgoing transfers op."), event.EventError)
				return
			}
			break
		}
	}
}

func (tp *TransferPoller) checkIncomingTransfers(depositRequests map[uint64]*store.StoredDepositRequest, pendingTransfers map[string]*store.PendingTransfer) {
	if len(depositRequests) == 0 {
		return
	}

	seed, err := tp.seedProvider.Seed()
	if err != nil {
		tp.em.Emit(errors.Wrap(err, "unable to get seed from seed provider in incoming transfers op."), event.EventError)
		return
	}

	depositAddresses := make(StringSet)
	depAddrs := make(Hashes, len(depositRequests))
	var i int
	for keyIndex, req := range depositRequests {
		addr, err := address.GenerateAddress(seed, keyIndex, req.SecurityLevel, false)
		if err != nil {
			tp.em.Emit(errors.Wrap(err, "unable to compute deposit address in incoming transfers op."), event.EventError)
			return
		}
		depAddrs[i] = addr
		i++
		depositAddresses[addr] = struct{}{}
	}

	spentAddresses := make(StringSet)
	for _, transfer := range pendingTransfers {
		bndl, err := store.PendingTransferToBundle(transfer)
		if err != nil {
			panic(err)
		}
		for j := range bndl {
			if bndl[j].Value < 0 {
				spentAddresses[bndl[j].Address] = struct{}{}
			}
		}
	}

	// get all bundles which operated on the current deposit addresses
	bndls, err := tp.api.GetBundlesFromAddresses(depAddrs, true)
	if err != nil {
		tp.em.Emit(errors.Wrap(err, "unable to fetch bundles from deposit addresses in incoming transfers op."), event.EventError)
		return
	}

	// create the events to emit in the event system
	tp.receiveEventfilter(tp.em, bndls, depositAddresses, spentAddresses)
}

type ReceiveEventTuple struct {
	Event  event.Event
	Bundle bundle.Bundle
}

// PerTailFilter filters receiving/received bundles by the bundle's tail transaction hash.
// Optionally takes in a bool flag indicating whether the first pass of the event filter
// should not emit any events.
func NewPerTailReceiveEventFilter(skipFirst ...bool) ReceiveEventFilter {
	var skipFirstEmitting bool
	if len(skipFirst) > 0 && skipFirst[0] {
		skipFirstEmitting = true
	}
	receivedFilter := make(map[string]struct{})
	receivingFilter := make(map[string]struct{})

	return func(em event.EventMachine, bndls bundle.Bundles, ownDepAddrs StringSet, ownSpentAddrs StringSet) {
		events := []ReceiveEventTuple{}

		receivingBundles := make(map[string]bundle.Bundle)
		receivedBundles := make(map[string]bundle.Bundle)

		// filter out transfers to own remainder addresses or where
		// a deposit is an input address (a spend from our own address)
		for _, bndl := range bndls {
			if err := bundle.ValidBundle(bndl); err != nil {
				continue
			}

			isSpendFromOwnAddr := false
			isTransferToOwnRemainderAddr := false

			// filter transfers to remainder addresses by checking
			// whether an input address is an own spent address
			for i := range bndl {
				if _, has := ownSpentAddrs[bndl[i].Address]; has && bndl[i].Value < 0 {
					isTransferToOwnRemainderAddr = true
					break
				}
			}
			// filter value transfers where a deposit address is an input
			for i := range bndl {
				if _, has := ownDepAddrs[bndl[i].Address]; has && bndl[i].Value < 0 {
					isSpendFromOwnAddr = true
					break
				}
			}
			if isTransferToOwnRemainderAddr || isSpendFromOwnAddr {
				continue
			}
			tailTx := bundle.TailTransactionHash(bndl)
			if *bndl[0].Persistence {
				receivedBundles[tailTx] = bndl
			} else {
				receivingBundles[tailTx] = bndl
			}
		}

		isValueTransfer := func(bndl bundle.Bundle) bool {
			isValue := false
			for _, tx := range bndl {
				if tx.Value > 0 || tx.Value < 0 {
					isValue = true
					break
				}
			}
			return isValue
		}

		// filter out bundles for which a previous event was emitted
		// and emit new events for the new bundles
		for tailTx, bndl := range receivingBundles {
			if _, has := receivingFilter[tailTx]; has {
				continue
			}
			receivingFilter[tailTx] = struct{}{}
			// determine whether the bundle is a value transfer.
			if isValueTransfer(bndl) {
				events = append(events, ReceiveEventTuple{EventReceivingDeposit, bndl})
				continue
			}
			events = append(events, ReceiveEventTuple{EventReceivedMessage, bndl})
		}

		for tailTx, bndl := range receivedBundles {
			if _, has := receivedFilter[tailTx]; has {
				continue
			}
			receivedFilter[tailTx] = struct{}{}
			if isValueTransfer(bndl) {
				events = append(events, ReceiveEventTuple{EventReceivedDeposit, bndl})
				continue
			}
			// if a message bundle got confirmed but an event was already emitted
			// during its receiving, we don't emit another event
			if _, has := receivingFilter[tailTx]; has {
				continue
			}
			events = append(events, ReceiveEventTuple{EventReceivedMessage, bndl})
		}

		// skip first emitting of events as multiple restarts of the same account
		// would yield the same events.
		if skipFirstEmitting {
			skipFirstEmitting = false
			return
		}

		for _, ev := range events {
			em.Emit(ev.Bundle, ev.Event)
		}
	}
}
