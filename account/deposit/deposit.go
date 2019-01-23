package deposit

import (
	"fmt"
	. "github.com/iotaledger/iota.go/trinary"
	"github.com/pkg/errors"
	"net/url"
	"strconv"
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
	return fmt.Sprintf("iota://%s/?t=%d&m=%v&am=%d", dc.Address, dc.TimeoutAt.Unix(), dc.MultiUse, dc.ExpectedAmount)
}

func ParseMagnetLink(s string) (*Conditions, error) {
	link, err := url.Parse(s)
	if err != nil {
		return nil, err
	}
	query := link.Query()
	cond := &Conditions{
		Address: link.Host,
	}
	expiresSeconds, err := strconv.ParseInt(query.Get(ConditionExpires), 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "invalid expire timestamp")
	}
	expire := time.Unix(expiresSeconds, 0)
	cond.TimeoutAt = &expire
	cond.MultiUse = query.Get(ConditionMultiUse) == "true"
	expectedAmount, err := strconv.ParseInt(query.Get(ConditionAmount), 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "invalid expected amount")
	}
	expectedAmountUint := uint64(expectedAmount)
	cond.ExpectedAmount = &expectedAmountUint
	return cond, nil
}

// Request defines a new deposit request against the account.
type Request struct {
	// The time after this deposit address becomes invalid.
	TimeoutAt *time.Time `json:"timeout_at,omitempty"`
	// Whether to expect multiple deposits to this address
	// in the given timeout.
	// If this flag is false, the deposit address is considered
	// in the input selection as soon as one deposit is available
	// (if the expected amount is set and also fulfilled)
	MultiUse bool `json:"multi_use,omitempty"`
	// The expected amount which gets deposited.
	// If the timeout is hit, the address is automatically
	// considered in the input selection.
	ExpectedAmount *uint64 `json:"expected_amount,omitempty"`
}
