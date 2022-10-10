package transactional

import (
	"database/sql"
	"errors"

	"github.com/smartystreets/messaging/v3"
)

type monitor interface {
	TransactionStarted(error)
	TransactionCommitted(error)
	TransactionRolledBack(error)
}
type logger interface {
	Printf(format string, args ...interface{})
}

type State struct {
	Tx     *sql.Tx
	Writer messaging.Writer
}

type handlerFunc func(state State) messaging.Handler

var errNilContext = errors.New("context must not be nil")
