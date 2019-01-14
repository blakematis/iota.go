package account

import (
	"github.com/iotaledger/iota.go/bundle"
	. "github.com/iotaledger/iota.go/trinary"
	"sync"
)

type EventMachine interface {
	// Emit emits the given event with the given data.
	Emit(data interface{}, event Event)
	// Register registers the given channel to receive values for the given event.
	Register(channel interface{}, event Event) uint64
	// Unregister unregisters the given channel to receive events.
	Unregister(uint64) error
}

// an event machine not emitting anything
type muteeventmachine struct{}

func (*muteeventmachine) Emit(data interface{}, event Event)               {}
func (*muteeventmachine) Register(channel interface{}, event Event) uint64 { return 0 }
func (*muteeventmachine) Unregister(id uint64) error                       { return nil }

func NewEventMachine() EventMachine {
	return &eventmachine{
		listeners: make(map[Event]map[uint64]interface{}),
	}
}

type eventmachine struct {
	listenersMu           sync.Mutex
	listeners             map[Event]map[uint64]interface{}
	nextID                uint64
	firstTransferPollMade bool
}

func (em *eventmachine) Emit(payload interface{}, event Event) {
	em.listenersMu.Lock()
	defer em.listenersMu.Unlock()
	listenersMap, ok := em.listeners[event]
	if !ok {
		return
	}
	for _, listener := range listenersMap {
		switch lis := listener.(type) {
		case chan bundle.Bundle:
			lis <- payload.(bundle.Bundle)
		case chan PromotionReattachmentEvent:
			lis <- payload.(PromotionReattachmentEvent)
		case chan ErrorEvent:
			lis <- payload.(ErrorEvent)
		case chan struct{}:
			lis <- struct{}{}
		}
	}
}

func (em *eventmachine) Register(channel interface{}, event Event) uint64 {
	em.listenersMu.Lock()
	id := em.nextID
	listenersMap, ok := em.listeners[event]
	if !ok {
		// allocate map
		listenersMap = make(map[uint64]interface{})
		em.listeners[event] = listenersMap
	}
	listenersMap[id] = channel
	em.nextID++
	em.listenersMu.Unlock()
	return id
}

func (em *eventmachine) Unregister(id uint64) error {
	em.listenersMu.Lock()
	for _, listenersMap := range em.listeners {
		delete(listenersMap, id)
	}
	em.listenersMu.Unlock()
	return nil
}

type Event byte

const (
	// emitted when a transfer was broadcasted
	EventSendingTransfer Event = iota
	// emitted when a broadcasted transfer got confirmed
	EventTransferConfirmed
	// emitted when a deposit is being received
	EventReceivingDeposit
	// emitted when a deposit is confirmed
	EventReceivedDeposit
	// emitted when a zero value transaction is received
	EventReceivedMessage
	// emitted when a promotion occurred
	EventPromotion
	// emitted when a reattachment occurred
	EventReattachment
	// emitted for errors of all kinds
	EventError
	// emitted when the account got shutdown cleanly
	EventShutdown
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

// ErrorEvent is emitted when an internal error inside the account occurs.
type ErrorEvent struct {
	Type  ErrorType `json:"error_type"`
	Error error     `json:"error"`
}

// ErrorType describes an error type
type ErrorType byte

// error event types
const (
	ErrorEventOutgoingTransfers ErrorType = iota
	ErrorEventIncomingTransfers
	ErrorPromoteTransfer
	ErrorReattachTransfer
	ErrorInternal
)

// EventListener handles channels and registration for events against an EventMachine.
// Use the builder methods to set this channel to listen to certain events.
// Once registered for events, you must listen for incoming events on the specific channel.
type EventListener struct {
	em               EventMachine
	ids              []uint64
	idsMu            sync.Mutex
	Promotion        chan PromotionReattachmentEvent
	Reattachment     chan PromotionReattachmentEvent
	Sending          chan bundle.Bundle
	Sent             chan bundle.Bundle
	ReceivingDeposit chan bundle.Bundle
	ReceivedDeposit  chan bundle.Bundle
	ReceivedMessage  chan bundle.Bundle
	InternalError    chan ErrorEvent
	Shutdown         chan struct{}
}

func NewEventListener(em EventMachine) *EventListener {
	return &EventListener{em: em, ids: []uint64{}}
}

// Close frees up all underlying channels from the EventMachine.
func (el *EventListener) Close() error {
	el.idsMu.Lock()
	defer el.idsMu.Unlock()
	for _, id := range el.ids {
		if err := el.em.Unregister(id); err != nil {
			return err
		}
	}
	return nil
}

// Promotions sets this listener up to listen for promotions.
func (el *EventListener) Promotions() *EventListener {
	el.Promotion = make(chan PromotionReattachmentEvent)
	el.ids = append(el.ids, el.em.Register(el.Promotion, EventPromotion))
	return el
}

// Reattachments sets this listener up to listen for reattachments.
func (el *EventListener) Reattachments() *EventListener {
	el.Reattachment = make(chan PromotionReattachmentEvent)
	el.ids = append(el.ids, el.em.Register(el.Reattachment, EventReattachment))
	return el
}

// Sends sets this listener up to listen for sent off bundles.
func (el *EventListener) Sends() *EventListener {
	el.Sending = make(chan bundle.Bundle)
	el.ids = append(el.ids, el.em.Register(el.Sending, EventSendingTransfer))
	return el
}

// ConfirmedSends sets this listener up to listen for sent off confirmed bundles.
func (el *EventListener) ConfirmedSends() *EventListener {
	el.Sent = make(chan bundle.Bundle)
	el.ids = append(el.ids, el.em.Register(el.Sent, EventTransferConfirmed))
	return el
}

// ReceivingDeposits sets this listener up to listen for incoming deposits which are not yet confirmed.
func (el *EventListener) ReceivingDeposits() *EventListener {
	el.ReceivingDeposit = make(chan bundle.Bundle)
	el.ids = append(el.ids, el.em.Register(el.ReceivingDeposit, EventReceivingDeposit))
	return el
}

// ReceivedDeposits sets this listener up to listen for received (confirmed) deposits.
func (el *EventListener) ReceivedDeposits() *EventListener {
	el.ReceivedDeposit = make(chan bundle.Bundle)
	el.ids = append(el.ids, el.em.Register(el.ReceivedDeposit, EventReceivedDeposit))
	return el
}

// ReceivedMessages sets this listener up to listen for incoming messages.
func (el *EventListener) ReceivedMessages() *EventListener {
	el.ReceivedMessage = make(chan bundle.Bundle)
	el.ids = append(el.ids, el.em.Register(el.ReceivedMessage, EventReceivedMessage))
	return el
}

// Shutdowns sets this listener up to listen for shutdown messages.
// A shutdown signal is normally only signaled once by the account on it's graceful termination.
func (el *EventListener) Shutdowns() *EventListener {
	el.Shutdown = make(chan struct{})
	el.ids = append(el.ids, el.em.Register(el.Shutdown, EventShutdown))
	return el
}

// InternalErrors sets this listener up to listen for internal account errors.
func (el *EventListener) InternalErrors() *EventListener {
	el.InternalError = make(chan ErrorEvent)
	el.ids = append(el.ids, el.em.Register(el.InternalError, EventError))
	return el
}

// All sets this listener up to listen to all account events.
func (el *EventListener) All() *EventListener {
	return el.Shutdowns().InternalErrors().
		Promotions().Reattachments().Sends().ConfirmedSends().
		ReceivedDeposits().ReceivingDeposits().ReceivedMessages()
}
