package streaming

import (
	"context"
	"time"

	"github.com/smarty/messaging/v4"
)

type workerFactory func(workerConfig) messaging.Listener

type workerConfig struct {
	Stream       messaging.Stream
	Subscription Subscription
	Handler      messaging.Handler
	SoftContext  context.Context
	HardContext  context.Context
	Logger       logger
	Monitor      monitor
	Now          func() time.Time
}
