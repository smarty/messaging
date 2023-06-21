package transactional

import (
	"database/sql"
	"errors"

	"github.com/smarty/messaging/v4"
)

func New(connector messaging.Connector, factory handlerFunc, options ...option) messaging.Handler {
	this := handler{connector: connector, factory: factory}

	for _, option := range Options.defaults(options...) {
		option(&this)
	}

	return this
}

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
