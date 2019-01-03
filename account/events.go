package account

import (
	"github.com/iotaledger/iota.go/bundle"
	. "github.com/iotaledger/iota.go/trinary"
)

type AccountEvent byte

const (
	// emitted when a transfer was broadcasted
	EventSendingTransfer AccountEvent = iota
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
)

type PromotionReattachmentEvent struct {
	OriginTailTxHash       Hash
	BundleHash             Hash
	PromotionTailTxHash    Hash
	ReattachmentTailTxHash Hash
}

type ErrorEvent struct {
	Type  ErrorType
	Error error
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

func ComposeEventListener(a *Account, events ...AccountEvent) AccountListener {
	listener := AccountListener{}
	for _, event := range events {
		switch event {
		case EventSendingTransfer:
			listener.Sending = BundleChannel(a.RegisterEventHandler(event))
		case EventTransferConfirmed:
			listener.Sent = BundleChannel(a.RegisterEventHandler(event))
		case EventReceivingDeposit:
			listener.ReceivingDeposit = BundleChannel(a.RegisterEventHandler(event))
		case EventReceivedDeposit:
			listener.ReceivedDeposit = BundleChannel(a.RegisterEventHandler(event))
		case EventReceivedMessage:
			listener.ReceivedMessage = BundleChannel(a.RegisterEventHandler(event))
		case EventPromotion:
			listener.Promotions = PromotionReattachmentChannel(a.RegisterEventHandler(event))
		case EventReattachment:
			listener.Reattachments = PromotionReattachmentChannel(a.RegisterEventHandler(event))
		}
	}
	return listener
}

type AccountListener struct {
	Promotions       <-chan PromotionReattachmentEvent
	Reattachments    <-chan PromotionReattachmentEvent
	Sending          <-chan bundle.Bundle
	Sent             <-chan bundle.Bundle
	ReceivingDeposit <-chan bundle.Bundle
	ReceivedDeposit  <-chan bundle.Bundle
	ReceivedMessage  <-chan bundle.Bundle
}
