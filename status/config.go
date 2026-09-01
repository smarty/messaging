package status

import (
	"context"
	"errors"
	"time"

	"github.com/smarty/messaging/v4"
)

func New(options ...option) Checker {
	var config configuration
	Options.apply(options...)(&config)
	return newDefaultStatusChecker(config)
}

type configuration struct {
	logger           logger
	connector        messaging.Connector
	topic            string
	failureTolerance time.Duration
	now              func() time.Time
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

// FailureTolerance sets how long Status tolerates consecutive probe failures
// (returning nil): once failures have persisted for this duration, Status
// reports the current error. Any success resets the window. A value of 0
// reports the first failure.
func (singleton) FailureTolerance(value time.Duration) option {
	return func(this *configuration) { this.failureTolerance = max(value, 0) }
}
func (singleton) Now(value func() time.Time) option {
	return func(this *configuration) { this.now = value }
}

func (singleton) apply(options ...option) option {
	return func(this *configuration) {
		for _, item := range Options.defaults(options...) {
			item(this)
		}
	}
}
func (singleton) defaults(options ...option) []option {
	const defaultFailureTolerance = time.Second * 30
	return append([]option{
		Options.Connector(nop{}),
		Options.Logger(nop{}),
		Options.Topic("amq.direct"),
		Options.FailureTolerance(defaultFailureTolerance),
		Options.Now(time.Now),
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
