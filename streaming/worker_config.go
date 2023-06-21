package streaming

import (
	"context"

	"github.com/smarty/messaging/v4"
)

type workerFactory func(workerConfig) messaging.Listener

type workerConfig struct {
	Streams      []messaging.Stream
	Subscription Subscription
	Handler      messaging.Handler
	SoftContext  context.Context
	HardContext  context.Context
}
