package streaming

import (
	"context"
	"time"

	"github.com/smarty/messaging/v3"
)

type Subscription struct {
	name               string
	streamName         string
	streamReplication  bool
	subscriptionTopics []string
	availableTopics    []string
	partition          uint64
	sequence           uint64
	handlers           []messaging.Handler
	bufferCapacity     uint16
	establishTopology  bool
	batchCapacity      uint16
	handleDelivery     bool
	deliveryToContext  bool
	bufferTimeout      time.Duration // the amount of time to rest and buffer between batches (instead of going as quickly as possible)
	reconnectDelay     time.Duration
	shutdownTimeout    time.Duration
	shutdownStrategy   ShutdownStrategy
}

func (this Subscription) streamConfig() messaging.StreamConfig {
	return messaging.StreamConfig{
		EstablishTopology: this.establishTopology,
		ExclusiveStream:   len(this.handlers) <= 1,
		BufferCapacity:    this.bufferCapacity,
		StreamName:        this.streamName,
		StreamReplication: this.streamReplication,
		Topics:            this.subscriptionTopics,
		AvailableTopics:   this.availableTopics,
		GroupName:         this.name,
		Partition:         this.partition,
		Sequence:          this.sequence,
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
