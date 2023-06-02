package transactional

import "github.com/smarty/messaging/v3"

func New(connector messaging.Connector, factory handlerFunc, options ...option) messaging.Handler {
	this := handler{connector: connector, factory: factory}

	for _, item := range Options.defaults(options...) {
		item(&this)
	}

	return this
}

var Options singleton

type singleton struct{}
type option func(*handler)

func (singleton) Logger(value logger) option {
	return func(this *handler) { this.logger = value }
}
func (singleton) Monitor(value monitor) option {
	return func(this *handler) { this.monitor = value }
}

func (singleton) defaults(options ...option) []option {
	var defaultLogger = nop{}
	var defaultMonitor = nop{}

	return append([]option{
		Options.Logger(defaultLogger),
		Options.Monitor(defaultMonitor),
	}, options...)
}

type nop struct{}

func (nop) Printf(_ string, _ ...any) {}

func (nop) TransactionStarted(_ error)    {}
func (nop) TransactionCommitted(_ error)  {}
func (nop) TransactionRolledBack(_ error) {}
