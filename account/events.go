package account

import (
	"github.com/iotaledger/iota.go/bundle"
	. "github.com/iotaledger/iota.go/trinary"
	"sync"
)

type EventMachine interface {
	// Emit emits the given event with the given data.
	Emit(data interface{}, event Event)
	// Register creates a new EventChannel listening to the given Event.
	Register(event Event) EventChannel
	// InternalAccountErrors returns a channel from which internal errors can be received
	// from processes (in-/out going transfer polling, promotion/reattachment etc.).
	InternalAccountErrors() <-chan ErrorEvent
}

// an event machine not emitting anything
type muteeventmachine struct{}

func (*muteeventmachine) Emit(data interface{}, event Event)       {}
func (*muteeventmachine) Register(event Event) EventChannel        { return nil }
func (*muteeventmachine) InternalAccountErrors() <-chan ErrorEvent { return nil }

func NewEventMachine() EventMachine {
	return &eventmachine{
		listeners: make(map[Event][]EventChannel),
		errors:    make(chan ErrorEvent),
	}
}

type eventmachine struct {
	listenersMu           sync.Mutex
	listeners             map[Event][]EventChannel
	firstTransferPollMade bool
	errors                chan ErrorEvent
}

func (em *eventmachine) Emit(payload interface{}, event Event) {
	if event == EventError {
		em.errors <- payload.(ErrorEvent)
		return
	}

	eventListeners := em.listeners[event]
	for _, listener := range eventListeners {
		select {
		case listener.Channel() <- payload:
		default:
		}
	}
}

func (em *eventmachine) Register(event Event) EventChannel {
	em.listenersMu.Lock()
	eventListeners := em.listeners[event]
	channel := eventchannel{make(chan interface{})}
	em.listeners[event] = append(eventListeners, channel)
	em.listenersMu.Unlock()
	return channel
}

func (em *eventmachine) InternalAccountErrors() <-chan ErrorEvent {
	return em.errors
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

type PromotionReattachmentEvent struct {
	OriginTailTxHash       Hash `json:"original_tail_tx_hash"`
	BundleHash             Hash `json:"bundle_hash"`
	PromotionTailTxHash    Hash `json:"promotion_tail_tx_hash"`
	ReattachmentTailTxHash Hash `json:"reattachment_tail_tx_hash"`
}

type ErrorEvent struct {
	Type  ErrorType `json:"error_type"`
	Error error     `json:"error"`
}

type ErrorType byte

const (
	ErrorEventOutgoingTransfers ErrorType = iota
	ErrorEventIncomingTransfers
	ErrorPromoteTransfer
	ErrorReattachTransfer
	ErrorInternal
)

type EventChannel interface {
	Channel() chan interface{}
}

type eventchannel struct {
	channel chan interface{}
}

func (ec eventchannel) Channel() chan interface{} {
	return ec.channel
}

func BundleChannel(channel EventChannel) <-chan bundle.Bundle {
	ch := make(chan bundle.Bundle)
	go func() {
		for {
			bndl, ok := <-channel.Channel()
			if !ok {
				return
			}
			ch <- bndl.(bundle.Bundle)
		}
	}()
	return ch
}

func PromotionReattachmentChannel(channel EventChannel) <-chan PromotionReattachmentEvent {
	ch := make(chan PromotionReattachmentEvent)
	go func() {
		for {
			bndl, ok := <-channel.Channel()
			if !ok {
				return
			}
			ch <- bndl.(PromotionReattachmentEvent)
		}
	}()
	return ch
}

func SignalChannel(channel EventChannel) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		for {
			bndl, ok := <-channel.Channel()
			if !ok {
				return
			}
			ch <- bndl.(struct{})
		}
	}()
	return ch
}

type EventListener struct {
	em               EventMachine
	Promotion        <-chan PromotionReattachmentEvent
	Reattachment     <-chan PromotionReattachmentEvent
	Sending          <-chan bundle.Bundle
	Sent             <-chan bundle.Bundle
	ReceivingDeposit <-chan bundle.Bundle
	ReceivedDeposit  <-chan bundle.Bundle
	ReceivedMessage  <-chan bundle.Bundle
	Shutdown         <-chan struct{}
}

func NewEventListener(em EventMachine) *EventListener {
	return &EventListener{em: em}
}

// Promotions sets this listener up to listen for promotions.
func (el *EventListener) Promotions() *EventListener {
	el.Promotion = PromotionReattachmentChannel(el.em.Register(EventPromotion))
	return el
}

// Reattachments sets this listener up to listen for reattachments.
func (el *EventListener) Reattachments() *EventListener {
	el.Reattachment = PromotionReattachmentChannel(el.em.Register(EventReattachment))
	return el
}

// Sends sets this listener up to listen for sent off bundles.
func (el *EventListener) Sends() *EventListener {
	el.Sending = BundleChannel(el.em.Register(EventSendingTransfer))
	return el
}

// ConfirmedSends sets this listener up to listen for sent off confirmed bundles.
func (el *EventListener) ConfirmedSends() *EventListener {
	el.Sent = BundleChannel(el.em.Register(EventTransferConfirmed))
	return el
}

// ReceivingDeposits sets this listener up to listen for sent off confirmed bundles.
func (el *EventListener) ReceivingDeposits() *EventListener {
	el.ReceivingDeposit = BundleChannel(el.em.Register(EventReceivingDeposit))
	return el
}

func (el *EventListener) ReceivedDeposits() *EventListener {
	el.ReceivedDeposit = BundleChannel(el.em.Register(EventReceivedDeposit))
	return el
}

func (el *EventListener) ReceivedMessages() *EventListener {
	el.ReceivedMessage = BundleChannel(el.em.Register(EventReceivedMessage))
	return el
}

func (el *EventListener) Shutdowns() *EventListener {
	el.Shutdown = SignalChannel(el.em.Register(EventShutdown))
	return el
}

func (el *EventListener) All() *EventListener {
	return el.Shutdowns().
		Promotions().Reattachments().Sends().ConfirmedSends().
		ReceivedDeposits().ReceivingDeposits().ReceivedMessages()
}
