package streaming

import (
	"context"
	"time"

	"github.com/smarty/messaging/v4"
)

type Subscription struct {
	streamConfigs     []messaging.StreamConfig
	handlers          []messaging.Handler
	bufferCapacity    uint16
	batchCapacity     uint16
	handleDelivery    bool
	deliveryToContext bool
	bufferTimeout     time.Duration // the amount of time to rest and buffer between batches (instead of going as quickly as possible)
	reconnectDelay    time.Duration
	shutdownTimeout   time.Duration
	shutdownStrategy  ShutdownStrategy
}

func (this Subscription) hardShutdown(potentialParent context.Context) (context.Context, context.CancelFunc) {
	if this.shutdownStrategy == ShutdownStrategyImmediate {
		return potentialParent, func() {}
	}

	return context.WithCancel(context.Background())
}

type ShutdownStrategy int

const (
	ShutdownStrategyCurrentBatch ShutdownStrategy = iota
	ShutdownStrategyImmediate
	ShutdownStrategyDrain
)
