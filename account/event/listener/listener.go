package listener

import (
	"github.com/iotaledger/iota.go/account/event"
	"github.com/iotaledger/iota.go/account/plugins/promoter"
	"github.com/iotaledger/iota.go/account/plugins/transfer/poller"
	"github.com/iotaledger/iota.go/bundle"
	"sync"
)

// EventListener handles channels and registration for events against an EventMachine.
// Use the builder methods to set this channel to listen to certain events.
// Once registered for events, you must listen for incoming events on the specific channel.
type EventListener struct {
	em               event.EventMachine
	ids              []uint64
	idsMu            sync.Mutex
	Promotion        chan promoter.PromotionReattachmentEvent
	Reattachment     chan promoter.PromotionReattachmentEvent
	Sending          chan bundle.Bundle
	Sent             chan bundle.Bundle
	ReceivingDeposit chan bundle.Bundle
	ReceivedDeposit  chan bundle.Bundle
	ReceivedMessage  chan bundle.Bundle
	InternalError    chan error
	Shutdown         chan struct{}
}

func NewEventListener(em event.EventMachine) *EventListener {
	return &EventListener{em: em, ids: []uint64{}}
}

// Close frees up all underlying channels from the EventMachine.
func (el *EventListener) Close() error {
	el.idsMu.Lock()
	defer el.idsMu.Unlock()
	for _, id := range el.ids {
		if err := el.em.UnregisterListener(id); err != nil {
			return err
		}
	}
	return nil
}

func connectBundleChannel(source chan interface{}) chan bundle.Bundle {
	dest := make(chan bundle.Bundle)
	go func() {
		for s := range source {
			dest <- s.(bundle.Bundle)
		}

	}()
	return dest
}

func connectPromotionReattachmentChannel(source chan interface{}) chan promoter.PromotionReattachmentEvent {
	dest := make(chan promoter.PromotionReattachmentEvent)
	go func() {
		for s := range source {
			dest <- s.(promoter.PromotionReattachmentEvent)
		}

	}()
	return dest
}

func connectSignalChannel(source chan interface{}) chan struct{} {
	dest := make(chan struct{})
	go func() {
		for s := range source {
			dest <- s.(struct{})
		}
	}()
	return dest
}

func connectErrorChannel(source chan interface{}) chan error {
	dest := make(chan error)
	go func() {
		for s := range source {
			dest <- s.(error)
		}
	}()
	return dest
}

// Promotions sets this listener up to listen for promotions.
func (el *EventListener) Promotions() *EventListener {
	genChannel := make(chan interface{})
	el.Promotion = connectPromotionReattachmentChannel(genChannel)
	el.ids = append(el.ids, el.em.RegisterListener(genChannel, promoter.EventPromotion))
	return el
}

// Reattachments sets this listener up to listen for reattachments.
func (el *EventListener) Reattachments() *EventListener {
	genChannel := make(chan interface{})
	el.Reattachment = connectPromotionReattachmentChannel(genChannel)
	el.ids = append(el.ids, el.em.RegisterListener(genChannel, promoter.EventReattachment))
	return el
}

// Sends sets this listener up to listen for sent off bundles.
func (el *EventListener) Sends() *EventListener {
	genChannel := make(chan interface{})
	el.Sending = connectBundleChannel(genChannel)
	el.ids = append(el.ids, el.em.RegisterListener(genChannel, event.EventSendingTransfer))
	return el
}

// ConfirmedSends sets this listener up to listen for sent off confirmed bundles.
func (el *EventListener) ConfirmedSends() *EventListener {
	genChannel := make(chan interface{})
	el.Sent = connectBundleChannel(genChannel)
	el.ids = append(el.ids, el.em.RegisterListener(genChannel, poller.EventTransferConfirmed))
	return el
}

// ReceivingDeposits sets this listener up to listen for incoming deposits which are not yet confirmed.
func (el *EventListener) ReceivingDeposits() *EventListener {
	genChannel := make(chan interface{})
	el.ReceivingDeposit = connectBundleChannel(genChannel)
	el.ids = append(el.ids, el.em.RegisterListener(genChannel, poller.EventReceivingDeposit))
	return el
}

// ReceivedDeposits sets this listener up to listen for received (confirmed) deposits.
func (el *EventListener) ReceivedDeposits() *EventListener {
	genChannel := make(chan interface{})
	el.ReceivedDeposit = connectBundleChannel(genChannel)
	el.ids = append(el.ids, el.em.RegisterListener(genChannel, poller.EventReceivedDeposit))
	return el
}

// ReceivedMessages sets this listener up to listen for incoming messages.
func (el *EventListener) ReceivedMessages() *EventListener {
	genChannel := make(chan interface{})
	el.ReceivedMessage = connectBundleChannel(genChannel)
	el.ids = append(el.ids, el.em.RegisterListener(genChannel, poller.EventReceivedMessage))
	return el
}

// Shutdowns sets this listener up to listen for shutdown messages.
// A shutdown signal is normally only signaled once by the account on it's graceful termination.
func (el *EventListener) Shutdowns() *EventListener {
	genChannel := make(chan interface{})
	el.Shutdown = connectSignalChannel(genChannel)
	el.ids = append(el.ids, el.em.RegisterListener(genChannel, event.EventShutdown))
	return el
}

// InternalErrors sets this listener up to listen for internal account errors.
func (el *EventListener) InternalErrors() *EventListener {
	genChannel := make(chan interface{})
	el.InternalError = connectErrorChannel(genChannel)
	el.ids = append(el.ids, el.em.RegisterListener(genChannel, event.EventError))
	return el
}

// All sets this listener up to listen to all account events.
func (el *EventListener) All() *EventListener {
	return el.Shutdowns().InternalErrors().
		Promotions().Reattachments().Sends().ConfirmedSends().
		ReceivedDeposits().ReceivingDeposits().ReceivedMessages()
}
