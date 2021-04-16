package streaming

import (
	"math"
	"time"

	"github.com/smartystreets/messaging/v3"
)

func NewSubscription(options ...subscriptionOption) Subscription {
	this := Subscription{}
	SubscriptionOptions.apply(options...)(&this)
	return this
}

var SubscriptionOptions subscriptionSingleton

type subscriptionSingleton struct{}
type subscriptionOption func(*Subscription)

func (subscriptionSingleton) AddStream(value messaging.StreamConfig) subscriptionOption {
	return func(this *Subscription) { this.streamConfigs = append(this.streamConfigs, value) }
}
func (subscriptionSingleton) AddWorkers(values ...messaging.Handler) subscriptionOption {
	return func(this *Subscription) { this.handlers = append(this.handlers, values...) }
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
	return append([]subscriptionOption{
		SubscriptionOptions.BufferCapacity(1),
		SubscriptionOptions.BatchCapacity(1),
		SubscriptionOptions.BufferDelayBetweenBatches(0),
		SubscriptionOptions.FullDeliveryToHandler(false),
		SubscriptionOptions.FullDeliveryToContext(false),
		SubscriptionOptions.ReconnectDelay(time.Second * 5),
		SubscriptionOptions.ShutdownStrategy(ShutdownStrategyDrain, time.Second*5),
	}, options...)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func NewStreamConfig() messaging.StreamConfig {
	return messaging.StreamConfig{}
}

var StreamOptions streamSingleton

type streamSingleton struct{}
type streamOption func(*messaging.StreamConfig)

func (streamSingleton) EstablishTopology(value bool) streamOption {
	return func(this *messaging.StreamConfig) { this.EstablishTopology = value }
}
func (streamSingleton) BufferCapacity(value uint16) streamOption {
	return func(this *messaging.StreamConfig) { this.BufferCapacity = value }
}
func (streamSingleton) MaxMessageBytes(value uint32) streamOption {
	return func(this *messaging.StreamConfig) { this.MaxMessageBytes = value }
}
func (streamSingleton) StreamName(value string) streamOption {
	return func(this *messaging.StreamConfig) { this.StreamName = value }
}
func (streamSingleton) GroupName(value string) streamOption {
	return func(this *messaging.StreamConfig) { this.GroupName = value }
}
func (streamSingleton) Topics(value ...string) streamOption {
	return func(this *messaging.StreamConfig) { this.Topics = value }
}
func (streamSingleton) Partition(value uint64) streamOption {
	return func(this *messaging.StreamConfig) { this.Partition = value }
}
func (streamSingleton) Sequence(value uint64) streamOption {
	return func(this *messaging.StreamConfig) { this.Sequence = value }
}

func (streamSingleton) apply(options ...streamOption) streamOption {
	return func(this *messaging.StreamConfig) {
		for _, option := range StreamOptions.defaults(options...) {
			option(this)
		}
	}
}
func (streamSingleton) defaults(options ...streamOption) []streamOption {
	return append([]streamOption{
		StreamOptions.EstablishTopology(true),
		StreamOptions.BufferCapacity(1),
		StreamOptions.MaxMessageBytes(1024 * 1024),
		StreamOptions.StreamName(""),
		StreamOptions.GroupName(""),
		StreamOptions.Topics(),
		StreamOptions.Partition(0),
		StreamOptions.Sequence(0),
	}, options...)
}
