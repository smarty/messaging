package retry

import "errors"

type monitor interface {
	HandleAttempted(attempt int, resultError any)
}
type logger interface {
	Printf(format string, args ...any)
}

var ErrMaxRetriesExceeded = errors.New("maximum number of retry attempts exceeded")
