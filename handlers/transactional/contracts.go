package transactional

import (
	"database/sql"

	"github.com/smartystreets/messaging/v3"
)

func New(connector messaging.Connector, factory handlerFunc, options ...option) messaging.Handler {
	this := handler{connector: connector, factory: factory}

	for _, option := range Options.defaults(options...) {
		option(&this)
	}

	return this
}

type Monitor interface {
	Begin(error)
	Commit(error)
	Rollback()
}

type State struct {
	Tx     *sql.Tx
	Writer messaging.Writer
}

type handlerFunc func(state State) messaging.Handler