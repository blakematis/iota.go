package event

import (
	"sync"
)

type EventMachine interface {
	// Emit emits the given event with the given data.
	Emit(data interface{}, event Event)
	// Register registers the given channel to receive values for the given event.
	RegisterListener(channel chan interface{}, event Event) uint64
	// Unregister unregisters the given channel to receive events.
	UnregisterListener(uint64) error
}

func NewEventMachine() EventMachine {
	return &eventmachine{
		listeners: make(map[Event]map[uint64]chan interface{}),
	}
}

type eventmachine struct {
	listenersMu sync.Mutex
	listeners   map[Event]map[uint64]chan interface{}
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
		listener <- payload
	}
}

func (em *eventmachine) RegisterListener(channel chan interface{}, event Event) uint64 {
	em.listenersMu.Lock()
	id := em.nextID
	listenersMap, ok := em.listeners[event]
	if !ok {
		// allocate map
		listenersMap = make(map[uint64]chan interface{})
		em.listeners[event] = listenersMap
	}
	listenersMap[id] = channel
	em.nextID++
	em.listenersMu.Unlock()
	return id
}

func (em *eventmachine) UnregisterListener(id uint64) error {
	em.listenersMu.Lock()
	for _, listenersMap := range em.listeners {
		if channel, has := listenersMap[id]; has {
			close(channel)
			delete(listenersMap, id)
		}
	}
	em.listenersMu.Unlock()
	return nil
}

type Event int64

const ( // emitted when a transfer was broadcasted
	EventSendingTransfer Event = iota
	// emitted for internal errors of all kinds
	EventError
	// emitted when the account got shutdown cleanly
	EventShutdown
)

// an event machine not emitting anything
type DiscardEventMachine struct{}

func (*DiscardEventMachine) Emit(data interface{}, event Event)                            {}
func (*DiscardEventMachine) RegisterListener(channel chan interface{}, event Event) uint64 { return 0 }
func (*DiscardEventMachine) UnregisterListener(id uint64) error                            { return nil }
