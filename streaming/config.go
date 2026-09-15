package streaming

import (
	"context"
	"time"

	"github.com/smarty/messaging/v4"
)

func New(connector messaging.Connector, options ...option) messaging.ListenCloser {
	configuration := config{}
	Options.apply(options...)(&configuration)

	pool := newConnectionPool(connector)
	return newManager(pool, configuration.subscriptions, func(ctx context.Context, sub Subscription) messaging.Listener {
		return newSubscriber(pool, sub, ctx, newWorker, configuration.logger, configuration.monitor)
	}, configuration.logger)
}

type config struct {
	logger        logger
	monitor       monitor
	subscriptions []Subscription
}

var Options singleton

type singleton struct{}
type option func(*config)

func (singleton) Logger(value logger) option {
	return func(this *config) { this.logger = value }
}

// Monitor receives consumer events: stream opened or failed, stream closed,
// batch handled with its duration, batch acknowledged or failed, and forced
// shutdown. Each callback carries the stream (queue) name so one monitor can
// label metrics per subscription. Callbacks run on the consumer's goroutines
// and must return quickly.
func (singleton) Monitor(value monitor) option {
	return func(this *config) { this.monitor = value }
}
func (singleton) Subscriptions(values ...Subscription) option {
	return func(this *config) { this.subscriptions = append(this.subscriptions, values...) }
}

func (singleton) apply(options ...option) option {
	return func(this *config) {
		for _, item := range Options.defaults(options...) {
			item(this)
		}
	}
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

func (nop) StreamOpened(_ string, _ error)                {}
func (nop) StreamClosed(_ string)                         {}
func (nop) BatchHandled(_ string, _ int, _ time.Duration) {}
func (nop) BatchAcknowledged(_ string, _ int, _ error)    {}
func (nop) ShutdownForced(_ string)                       {}
