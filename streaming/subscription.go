package streaming

import (
	"context"
	"time"

	"github.com/smartystreets/messaging/v3"
)

type Subscription struct {
	name              string
	queue             string
	topics            []string
	handlers          []messaging.Handler
	bufferCapacity    uint16
	establishTopology bool
	batchCapacity     uint16
	handleDelivery    bool
	deliveryToContext bool
	bufferTimeout     time.Duration // the amount of time to rest and buffer between batches (instead of going as quickly as possible)
	reconnectDelay    time.Duration
	shutdownTimeout   time.Duration
	shutdownStrategy  ShutdownStrategy
}

func (this Subscription) streamConfig() messaging.StreamConfig {
	return messaging.StreamConfig{
		EstablishTopology: this.establishTopology,
		ExclusiveStream:   len(this.handlers) <= 1,
		BufferCapacity:    this.bufferCapacity,
		StreamName:        this.queue,
		Topics:            this.topics,
	}
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
