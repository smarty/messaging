package streaming

import (
	"context"

	"github.com/smartystreets/messaging/v3"
)

func New(connector messaging.Connector, options ...option) messaging.ListenCloser {
	config := config{}
	Options.apply(options...)(&config)

	pool := newConnectionPool(connector)
	return newManager(pool, config.subscriptions, func(ctx context.Context, sub Subscription) messaging.Listener {
		return newSubscriber(pool, sub, ctx, newWorker)
	})
}

type config struct {
	logger        logger
	subscriptions []Subscription
}

var Options singleton

type singleton struct{}
type option func(*config)

func (singleton) Logger(value logger) option {
	return func(this *config) { this.logger = value }
}
func (singleton) Subscriptions(values ...Subscription) option {
	return func(this *config) { this.subscriptions = append(this.subscriptions, values...) }
}

func (singleton) apply(options ...option) option {
	return func(this *config) {
		for _, option := range Options.defaults(options...) {
			option(this)
		}
	}
}
func (singleton) defaults(options ...option) []option {
	var defaultLogger = nop{}

	return append([]option{
		Options.Logger(defaultLogger),
	}, options...)
}

type nop struct{}

func (nop) Printf(_ string, _ ...any) {}
