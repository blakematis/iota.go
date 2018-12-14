package deposit

import (
	"fmt"
	. "github.com/iotaledger/iota.go/trinary"
	"time"
)

type Conditions struct {
	Request
	Address Hash `json:"address"`
}

type ConditionField string

const (
	ConditionExpires  = "t"
	ConditionMultiUse = "m"
	ConditionAmount   = "am"
)

func (dc *Conditions) URL() string {
	return fmt.Sprintf("iota://%s/?t=%d&m=%v&am=%d", dc.Address, dc.TimeoutOn.Unix(), dc.MultiUse, dc.ExpectedAmount)
}

type Request struct {
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
