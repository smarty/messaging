package sqltx

import (
	"database/sql"

	"github.com/smartystreets/messaging/v3"
)

func New(handle *sql.DB, callback handlerFunc, options ...option) messaging.Handler {
	config := configuration{Handle: handle, Callback: callback}

	for _, item := range Options.defaults(options...) {
		item(&config)
	}

	return newHandler(config)
}

type configuration struct {
	Handle         *sql.DB
	Callback       handlerFunc
	ReadOnly       bool
	IsolationLevel sql.IsolationLevel
	Logger         logger
	Monitor        monitor
}

var Options singleton

type singleton struct{}
type option func(*configuration)

func (singleton) Handle(value *sql.DB) option {
	return func(this *configuration) { this.Handle = value }
}
func (singleton) Callback(value handlerFunc) option {
	return func(this *configuration) { this.Callback = value }
}

func (singleton) ReadOnly(value bool) option {
	return func(this *configuration) { this.ReadOnly = value }
}
func (singleton) IsolationLevel(value sql.IsolationLevel) option {
	return func(this *configuration) { this.IsolationLevel = value }
}
func (singleton) Logger(value logger) option {
	return func(this *configuration) { this.Logger = value }
}
func (singleton) Monitor(value monitor) option {
	return func(this *configuration) { this.Monitor = value }
}

func (singleton) defaults(options ...option) []option {
	return append([]option{
		Options.ReadOnly(false),
		Options.IsolationLevel(sql.LevelReadCommitted),
		Options.Logger(&nop{}),
		Options.Monitor(&nop{}),
	}, options...)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type monitor interface {
	TransactionStarted(error)
	TransactionCommitted(error)
	TransactionRolledBack(error)
}
type logger interface {
	Printf(format string, args ...interface{})
}

type handlerFunc func(*sql.Tx) messaging.Handler

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type nop struct{}

func (*nop) Printf(_ string, _ ...interface{}) {}

func (*nop) TransactionStarted(_ error)    {}
func (*nop) TransactionCommitted(_ error)  {}
func (*nop) TransactionRolledBack(_ error) {}
