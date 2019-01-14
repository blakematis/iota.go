package account

import (
	"fmt"
	"github.com/pkg/errors"
)

var ErrEmptyRecipients = errors.New("recipients slice must be of size > 0")

type ErrMarkDepositAddr struct {
	actualError error
}

func (err ErrMarkDepositAddr) Error() string {
	return err.Error()
}

type ErrAccountPanic struct {
	internalError error
}

func (err ErrAccountPanic) Error() string {
	return fmt.Sprintf("severe account error (panic): %s", err.internalError.Error())
}

var ErrTimeoutNotSpecified = errors.New("deposit requests must have a timeout")
var ErrTimeoutTooLow = errors.New("deposit requests must at least have a timeout of >2 minutes")
var ErrAddrGeneratorStopped = errors.New("address generator goroutine is stopped")
