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
	severTimeout     time.Duration
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

// SeverTimeout bounds the wait, after the caller's context ends and the
// connection is severed, for the stalled probe to return. The rabbitmq
// transport fails every pending call on a closed connection, so the probe
// returns well inside the default. A transport that does not would otherwise
// park Status forever; past this bound the probe is abandoned with a warning.
// A zero or negative value is replaced with the default.
func (singleton) SeverTimeout(value time.Duration) option {
	return func(this *configuration) {
		if value <= 0 {
			value = defaultSeverTimeout
		}
		this.severTimeout = value
	}
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
		Options.SeverTimeout(defaultSeverTimeout),
		Options.Now(time.Now),
	}, options...)
}

// defaultSeverTimeout matches the rabbitmq adapter's close grace period: a
// severed connection fails its pending calls within that window.
const defaultSeverTimeout = time.Second * 5

type nop struct{}

func (n nop) Printf(string, ...any) {}
func (n nop) Connect(context.Context) (messaging.Connection, error) {
	return nil, errors.New("nop connector")
}
func (n nop) Close() error {
	return errors.New("nop connector")
}
