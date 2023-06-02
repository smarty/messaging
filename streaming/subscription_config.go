package streaming

import (
	"context"
	"math"
	"time"

	"github.com/smarty/messaging/v3"
)

func NewSubscription(streamName string, options ...subscriptionOption) Subscription {
	this := Subscription{streamName: streamName}
	SubscriptionOptions.apply(options...)(&this)
	return this
}

var SubscriptionOptions subscriptionSingleton

type subscriptionSingleton struct{}
type subscriptionOption func(*Subscription)

func (subscriptionSingleton) Name(value string) subscriptionOption {
	return func(this *Subscription) { this.name = value }
}
func (subscriptionSingleton) AddWorkers(values ...messaging.Handler) subscriptionOption {
	return func(this *Subscription) { this.handlers = append(this.handlers, values...) }
}
func (subscriptionSingleton) AddLegacyWorkers(values ...legacyHandler) subscriptionOption {
	return func(this *Subscription) {
		for _, handler := range values {
			this.handlers = append(this.handlers, legacyAdapter{inner: handler})
		}
	}
}
func (subscriptionSingleton) FullThrottle() subscriptionOption {
	return func(this *Subscription) { this.bufferCapacity = math.MaxUint16; this.batchCapacity = math.MaxUint16 }
}
func (subscriptionSingleton) BufferCapacity(value uint16) subscriptionOption {
	return func(this *Subscription) { this.bufferCapacity = value }
}
func (subscriptionSingleton) BatchCapacity(value uint16) subscriptionOption {
	return func(this *Subscription) { this.batchCapacity = value }
}
func (subscriptionSingleton) BufferDelayBetweenBatches(value time.Duration) subscriptionOption {
	return func(this *Subscription) { this.bufferTimeout = value }
}
func (subscriptionSingleton) EstablishTopology(value bool) subscriptionOption {
	return func(this *Subscription) { this.establishTopology = value }
}
func (subscriptionSingleton) StreamReplication(value bool) subscriptionOption {
	return func(this *Subscription) { this.streamReplication = value }
}
func (subscriptionSingleton) Topics(values ...string) subscriptionOption {
	return func(this *Subscription) { this.topics = values }
}
func (subscriptionSingleton) Partition(value uint64) subscriptionOption {
	return func(this *Subscription) { this.partition = value }
}
func (subscriptionSingleton) Sequence(value uint64) subscriptionOption {
	return func(this *Subscription) { this.sequence = value }
}
func (subscriptionSingleton) FullDeliveryToHandler(value bool) subscriptionOption {
	return func(this *Subscription) { this.handleDelivery = value }
}
func (subscriptionSingleton) FullDeliveryToContext(value bool) subscriptionOption {
	return func(this *Subscription) { this.deliveryToContext = value }
}
func (subscriptionSingleton) ReconnectDelay(value time.Duration) subscriptionOption {
	return func(this *Subscription) { this.reconnectDelay = value }
}
func (subscriptionSingleton) ShutdownStrategy(strategy ShutdownStrategy, timeout time.Duration) subscriptionOption {
	return func(this *Subscription) {
		switch strategy {
		case ShutdownStrategyImmediate, ShutdownStrategyCurrentBatch, ShutdownStrategyDrain:
			break
		default:
			panic("unrecognized shutdown strategy")
		}

		this.shutdownStrategy = strategy
		if strategy == ShutdownStrategyImmediate {
			timeout = 0
		}

		this.shutdownTimeout = timeout
	}
}

func (subscriptionSingleton) apply(options ...subscriptionOption) subscriptionOption {
	return func(this *Subscription) {
		for _, option := range SubscriptionOptions.defaults(options...) {
			option(this)
		}

		if length := len(this.handlers); length > int(this.bufferCapacity) {
			this.bufferCapacity = uint16(length)
		}

		if len(this.handlers) == 0 {
			panic("no workers configured")
		}
	}
}
func (subscriptionSingleton) defaults(options ...subscriptionOption) []subscriptionOption {
	const defaultBufferCapacity = 1
	const defaultBatchCapacity = 1
	const defaultBatchDelay = 0
	const defaultEstablishTopology = true
	const defaultPassFullDeliveryToHandler = false
	const defaultPassFullDeliveryToContext = false
	const defaultReconnectDelay = time.Second * 5
	const defaultShutdownStrategy = ShutdownStrategyDrain
	const defaultShutdownTimeout = time.Second * 5

	return append([]subscriptionOption{
		SubscriptionOptions.BufferCapacity(defaultBufferCapacity),
		SubscriptionOptions.BatchCapacity(defaultBatchCapacity),
		SubscriptionOptions.BufferDelayBetweenBatches(defaultBatchDelay),
		SubscriptionOptions.EstablishTopology(defaultEstablishTopology),
		SubscriptionOptions.StreamReplication(false),
		SubscriptionOptions.FullDeliveryToHandler(defaultPassFullDeliveryToHandler),
		SubscriptionOptions.FullDeliveryToContext(defaultPassFullDeliveryToContext),
		SubscriptionOptions.ReconnectDelay(defaultReconnectDelay),
		SubscriptionOptions.ShutdownStrategy(defaultShutdownStrategy, defaultShutdownTimeout),
	}, options...)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type legacyHandler interface{ Handle(messages ...any) }
type legacyAdapter struct{ inner legacyHandler }

func (this legacyAdapter) Handle(_ context.Context, messages ...any) {
	this.inner.Handle(messages...)
}
