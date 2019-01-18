package account

import (
	"github.com/iotaledger/iota.go/account/event"
	"github.com/iotaledger/iota.go/account/store"
	"github.com/iotaledger/iota.go/api"
	"github.com/iotaledger/iota.go/consts"
	. "github.com/iotaledger/iota.go/trinary"
	"time"
)

// InputSelectionFunc defines a function which given the account, transfer value and the flag balance check,
// computes the inputs for fulfilling the transfer or the usable balance of the account.
// The InputSelectionFunc must obey to the rules of conditional deposit requests to ensure consistency.
// It returns the computed balance/transfer value, inputs and the key indices to remove from the store.
type InputSelectionFunc func(acc *account, transferValue uint64, balanceCheck bool) (uint64, []api.Input, []uint64, error)

// Clock defines a source of time.
type Clock interface {
	Now() (time.Time, error)
}

type systemclock struct{}

func (rc *systemclock) Now() (time.Time, error) {
	return time.Now().UTC(), nil
}

func New(api *api.API, store store.Store) *Settings {
	return defaultSettings().API(api).Store(store)
}

// Settings defines settings used by an account.
type Settings struct {
	api                    *api.API
	store                  store.Store
	seedProv               SeedProvider
	mwm                    uint64
	depth                  uint64
	securityLevel          consts.SecurityLevel
	clock                  Clock
	inputSelectionStrategy InputSelectionFunc
	eventMachine           event.EventMachine
	plugins                []Plugin
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

// InputSelectionFunc sets the strategy to determine inputs and usable balance.
func (s *Settings) InputSelectionStrategy(strat InputSelectionFunc) *Settings {
	s.inputSelectionStrategy = strat
	return s
}

// PromotionReattachmentStrategy sets the strategy used to promote and reattach pending transfers.
func (s *Settings) With(plugins ...Plugin) *Settings {
	s.plugins = append(s.plugins, plugins...)
	return s
}

// WithEvents instructs the account to emit events using the given EventMachine.
func (s *Settings) WithEvents(em event.EventMachine) *Settings {
	s.eventMachine = em
	return s
}

func defaultSettings(setts ...*Settings) *Settings {
	if len(setts) == 0 {
		return &Settings{
			mwm: 14, depth: 3, securityLevel: consts.SecurityLevelMedium,
			clock:                  &systemclock{},
			inputSelectionStrategy: defaultInputSelection,
			eventMachine:           &event.DiscardEventMachine{},
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
	if opt.clock == nil {
		opt.clock = &systemclock{}
	}
	if opt.inputSelectionStrategy == nil {
		opt.inputSelectionStrategy = defaultInputSelection
	}
	return opt
}
