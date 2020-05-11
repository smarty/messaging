package transactional

import (
	"github.com/smartystreets/messaging/v3"
)

var Options singleton

type singleton struct{}
type option func(*handler)

func (singleton) Logger(value messaging.Logger) option {
	return func(this *handler) { this.logger = value }
}
func (singleton) Monitor(value Monitor) option {
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

func (nop) Printf(_ string, _ ...interface{}) {}
func (nop) Println(_ ...interface{})          {}

func (nop) Begin(_ error)  {}
func (nop) Commit(_ error) {}
func (nop) Rollback()      {}