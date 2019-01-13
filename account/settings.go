package account

import (
	"github.com/iotaledger/iota.go/account/store"
	"github.com/iotaledger/iota.go/api"
	"github.com/iotaledger/iota.go/bundle"
	"github.com/iotaledger/iota.go/consts"
	. "github.com/iotaledger/iota.go/trinary"
	"time"
)

// InputSelectionStrategy defines a function which given the account, transfer value and the flag balance check,
// computes the inputs for fulfilling the transfer or the usable balance of the account.
// The InputSelectionStrategy must obey to the rules of deposit conditions to ensure consistency.
type InputSelectionStrategy func(acc *account, transferValue uint64, balanceCheck bool) (uint64, []api.Input, []uint64, error)

// ReceiveEventFilter filters and creates events given the incoming bundles, deposit requests and spent addresses.
// It's the job of the ReceiveEventFilter to emit the appropriate events through the given EventMachine.
type ReceiveEventFilter func(eventMachine EventMachine, bndls bundle.Bundles, depAddrs StringSet, spentAddrs StringSet)

type ReceiveEventTuple struct {
	Event  Event
	Bundle bundle.Bundle
}

// StringSet is a set of strings.
type StringSet map[string]struct{}

// Clock defines a source of time.
type Clock interface {
	Now() (time.Time, error)
}

// PromotionReattachmentStrategy defines a function which given the account, tries to promote or reattach
// pending transfers of the account.
type PromotionReattachmentStrategy func(acc *account)

type systemclock struct{}

func (rc *systemclock) Now() (time.Time, error) {
	return time.Now().UTC(), nil
}

func NewBuilder(api *api.API, store store.Store) *Settings {
	return defaultSettings().API(api).Store(store)
}

// Settings defines options when instantiating a new account.
type Settings struct {
	api                            *api.API
	store                          store.Store
	seedProv                       SeedProvider
	mwm                            uint64
	depth                          uint64
	transferPollInterval           time.Duration
	promoteReattachInterval        time.Duration
	securityLevel                  consts.SecurityLevel
	clock                          Clock
	receiveEventFilter             ReceiveEventFilter
	inputSelectionStrategy         InputSelectionStrategy
	promoteReattachmentStrategy    PromotionReattachmentStrategy
	emitFirstTransferPollingEvents bool
	eventMachine                   EventMachine
}

// Build creates the account from the given settings.
func (s *Settings) Build() (Account, error) {
	settsCopy := *s
	return newAccount(&settsCopy)
}

// API sets the underlying API to use.
func (s *Settings) API(api *api.API) *Settings {
	s.api = api
	return s
}

// Store sets the underlying store to use.
func (s *Settings) Store(store store.Store) *Settings {
	s.store = store
	return s
}

// SeedProvider sets the underlying SeedProvider to use.
func (s *Settings) SeedProvider(seedProv SeedProvider) *Settings {
	s.seedProv = seedProv
	return s
}

// Seed sets the underlying seed to use.
func (s *Settings) Seed(seed Trytes) *Settings {
	s.seedProv = NewInMemorySeedProvider(seed)
	return s
}

// MWM sets the minimum weight magnitude used to send transactions.
func (s *Settings) MWM(mwm uint64) *Settings {
	s.mwm = mwm
	return s
}

// Depth sets the depth used when searching for transactions to approve.
func (s *Settings) Depth(depth uint64) *Settings {
	s.depth = depth
	return s
}

// TransferPollInterval sets the interval in seconds at which in/outbound transfers are checked.
func (s *Settings) TransferPollInterval(seconds uint64) *Settings {
	s.transferPollInterval = time.Duration(seconds) * time.Second
	return s
}

// PromoteReattachInterval sets the interval in seconds at which promotion and reattachments occur.
func (s *Settings) PromoteReattachInterval(seconds uint64) *Settings {
	s.promoteReattachInterval = time.Duration(seconds) * time.Second
	return s
}

// The overall security level used by the account.
// The security level must not be changed in the account's lifetime.
// Consider creating accounts with different seeds and other security levels instead.
func (s *Settings) SecurityLevel(level consts.SecurityLevel) *Settings {
	s.securityLevel = level
	return s
}

// Clock sets the clock to use to get time information.
func (s *Settings) Clock(clock Clock) *Settings {
	s.clock = clock
	return s
}

// ReceiveEventFilter sets the filter which takes care of filtering incoming transfer events.
func (s *Settings) ReceiveEventFilter(filter ReceiveEventFilter) *Settings {
	s.receiveEventFilter = filter
	return s
}

// InputSelectionStrategy sets the strategy to determine inputs and usable balance.
func (s *Settings) InputSelectionStrategy(strat InputSelectionStrategy) *Settings {
	s.inputSelectionStrategy = strat
	return s
}

// PromotionReattachmentStrategy sets the strategy used to promote and reattach pending transfers.
func (s *Settings) PromotionReattachmentStrategy(strat PromotionReattachmentStrategy) *Settings {
	s.promoteReattachmentStrategy = strat
	return s
}

// EmitFirstTransferPollingEvents sets whether to emit events for incoming transfers in the first transfer polling.
// This option should not be set to true, if the lib user is only interested into
// transfers happening against deposit addresses, after account instantiation.
func (s *Settings) EmitFirstTransferPollingEvents(emit bool) *Settings {
	s.emitFirstTransferPollingEvents = emit
	return s
}

// WithEvents instructs the account to emit events using the given EventMachine.
func (s *Settings) WithEvents(em EventMachine) *Settings {
	s.eventMachine = em
	return s
}

func defaultSettings(setts ...*Settings) *Settings {
	if len(setts) == 0 {
		return &Settings{
			mwm: 14, depth: 3, securityLevel: consts.SecurityLevelMedium,
			transferPollInterval:        time.Duration(30) * time.Second,
			promoteReattachInterval:     time.Duration(1) * time.Minute,
			clock:                       &systemclock{},
			receiveEventFilter:          NewPerTailReceiveEventFilter(),
			inputSelectionStrategy:      defaultInputSelectionStrategy,
			promoteReattachmentStrategy: defaultPromoteReattachmentStrategy,
			eventMachine:                &muteeventmachine{},
		}
	}
	defaultValue := func(val uint64, should uint64) uint64 {
		if val == 0 {
			return should
		}
		return val
	}
	opt := setts[0]
	if opt.securityLevel == 0 {
		opt.securityLevel = consts.SecurityLevelMedium
	}
	opt.depth = defaultValue(opt.depth, 3)
	opt.mwm = defaultValue(opt.mwm, 14)
	if opt.transferPollInterval == 0 {
		opt.transferPollInterval = time.Duration(30) * time.Second
	}
	if opt.promoteReattachInterval == 0 {
		opt.promoteReattachInterval = time.Duration(1) * time.Minute
	}
	if opt.clock == nil {
		opt.clock = &systemclock{}
	}
	if opt.receiveEventFilter == nil {
		opt.receiveEventFilter = NewPerTailReceiveEventFilter()
	}
	if opt.inputSelectionStrategy == nil {
		opt.inputSelectionStrategy = defaultInputSelectionStrategy
	}
	return opt
}
