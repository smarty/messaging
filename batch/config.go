package batch

import (
	"github.com/smartystreets/messaging/v3"
)

type configuration struct {
	Connector      messaging.Connector
	ReuseWriter    bool
	CloseConnector bool
}

func NewWriter(transport messaging.Connector, options ...option) messaging.Writer {
	var config configuration
	options = append(options, Options.Connector(transport))
	Options.apply(options...)(&config)
	return newWriter(config)
}

var Options singleton

type singleton struct{}
type option func(*configuration)

func (singleton) Connector(value messaging.Connector) option {
	return func(this *configuration) { this.Connector = value }
}
func (singleton) ReuseWriter(value bool) option {
	return func(this *configuration) { this.ReuseWriter = value }
}
func (singleton) CloseConnector(value bool) option {
	return func(this *configuration) { this.CloseConnector = value }
}

func (singleton) apply(options ...option) option {
	return func(this *configuration) {
		for _, item := range Options.defaults(options...) {
			item(this)
		}
	}
}
func (singleton) defaults(options ...option) []option {
	return append([]option{
		Options.ReuseWriter(true),
		Options.CloseConnector(true),
	}, options...)
}
