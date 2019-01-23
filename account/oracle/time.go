package oracle

import (
	"fmt"
	"github.com/iotaledger/iota.go/account"
	"github.com/iotaledger/iota.go/account/deposit"
	"time"
)

const dateFormat = "2006-02-01 15:04:05"

// NewTimeDecider creates a new OracleSource which uses the current time to decide whether
// it makes sense to send a transaction. remainingTimeThreshold defines the maximum allowed
// remaining time between now and the conditions' timeout.
func NewTimeDecider(clock account.Clock, remainingTimeThreshold time.Duration) OracleSource {
	return &timedecider{clock, remainingTimeThreshold}
}

type timedecider struct {
	clock                  account.Clock
	remainingTimeThreshold time.Duration
}

func (td *timedecider) Ok(conds *deposit.Conditions) (bool, string, error) {
	now, err := td.clock.Now()
	if err != nil {
		return false, "", err
	}

	if now.After(*conds.TimeoutAt) {
		msg := fmt.Sprintf("conditions expired on %s, it's currently %s", conds.TimeoutAt.Format(dateFormat), now.Format(dateFormat))
		return false, msg, nil
	}

	if now.Add(td.remainingTimeThreshold).After(*conds.TimeoutAt) {
		formatted := conds.TimeoutAt.Format(dateFormat)
		nowFormatted := now.Add(td.remainingTimeThreshold).Format(dateFormat)
		msg := fmt.Sprintf("conditions will have expired before the remaining time threshold (%s < %s)", formatted, nowFormatted)
		return false, msg, nil
	}
	return true, "", nil
}
