package streaming

import (
	"context"

	"github.com/smarty/messaging/v3"
)

type workerFactory func(workerConfig) messaging.Listener

type workerConfig struct {
	Stream       messaging.Stream
	Subscription Subscription
	Handler      messaging.Handler
	SoftContext  context.Context
	HardContext  context.Context
}
