package status

import (
	"context"
	"errors"

	"github.com/smarty/messaging/v3"
)

func New(options ...option) Checker {
	var config configuration
	Options.apply(options...)(&config)
	return newDefaultStatusChecker(config)
}

type configuration struct {
	logger    logger
	connector messaging.Connector
	topic     string
}

var Options singleton

type singleton struct{}
type option func(*configuration)

func (singleton) Logger(logger logger) option {
	return func(this *configuration) { this.logger = logger }
}
func (singleton) Connector(connector messaging.Connector) option {
	return func(this *configuration) { this.connector = connector }
}
func (singleton) Topic(topic string) option {
	return func(this *configuration) { this.topic = topic }
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
		Options.Connector(nop{}),
		Options.Logger(nop{}),
		Options.Topic("amq.direct"),
	}, options...)
}

type nop struct{}

func (n nop) Printf(string, ...any) {}
func (n nop) Connect(context.Context) (messaging.Connection, error) {
	return nil, errors.New("nop connector")
}
func (n nop) Close() error {
	return errors.New("nop connector")
}
