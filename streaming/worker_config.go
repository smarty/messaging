package streaming

import (
	"context"

	"github.com/smartystreets/messaging/v3"
)

type workerFactory func(workerConfig) messaging.Listener

type workerConfig struct {
	Streams      []messaging.Stream
	Subscription Subscription
	Handler      messaging.Handler
	SoftContext  context.Context
	HardContext  context.Context
}
