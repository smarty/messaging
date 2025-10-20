package streaming

import (
	"context"
	"testing"

	"github.com/smarty/gunit"
	"github.com/smarty/gunit/assert/should"

	"github.com/smarty/messaging/v3"
)

func TestSubscriptionConfigFixture(t *testing.T) {
	gunit.Run(new(SubscriptionConfigFixture), t)
}

type SubscriptionConfigFixture struct {
	*gunit.Fixture

	legacyHandleMessages []any
}

func (this *SubscriptionConfigFixture) Setup() {
}

func (this *SubscriptionConfigFixture) TestWhenNoHandlersAreConfigured_ItShouldPanic() {
	this.So(func() { NewSubscription("queue") }, should.Panic)
}

func (this *SubscriptionConfigFixture) TestWhenLegacyHandlerIsProvided_HandlerShouldBeAdapted() {
	subscription := NewSubscription("queue", SubscriptionOptions.AddLegacyWorkers(this))

	subscription.handlers[0].Handle(context.Background(), 0, 1, 2)

	this.So(this.legacyHandleMessages, should.Equal, []any{0, 1, 2})
}
func (this *SubscriptionConfigFixture) Handle(messages ...any) {
	this.legacyHandleMessages = messages
}

func (this *SubscriptionConfigFixture) TestWhenValuesAreProvided_SubscriptionShouldHaveValues() {
	subscription := NewSubscription("queue",
		SubscriptionOptions.Name("name"),
		SubscriptionOptions.AddWorkers(nil),
		SubscriptionOptions.Topics("topic1", "topic2"),
		SubscriptionOptions.AvailableTopics("topic1", "topic2", "topic3"),
		SubscriptionOptions.BatchCapacity(1),
		SubscriptionOptions.BufferCapacity(2),
		SubscriptionOptions.BufferDelayBetweenBatches(3),
		SubscriptionOptions.EstablishTopology(true),
		SubscriptionOptions.StreamReplication(true),
		SubscriptionOptions.FullDeliveryToHandler(true),
		SubscriptionOptions.ReconnectDelay(5),
		SubscriptionOptions.ShutdownStrategy(ShutdownStrategyCurrentBatch, 4),
		SubscriptionOptions.Partition(6),
		SubscriptionOptions.Sequence(7),
	)

	this.So(subscription, should.Equal, Subscription{
		name:               "name",
		streamName:         "queue",
		streamReplication:  true,
		subscriptionTopics: []string{"topic1", "topic2"},
		availableTopics:    []string{"topic3"},
		handlers:           []messaging.Handler{nil},
		bufferCapacity:     2,
		establishTopology:  true,
		batchCapacity:      1,
		handleDelivery:     true,
		bufferTimeout:      3,
		reconnectDelay:     5,
		shutdownStrategy:   ShutdownStrategyCurrentBatch,
		shutdownTimeout:    4,
		partition:          6,
		sequence:           7,
	})
}

func (this *SubscriptionConfigFixture) TestWhenUnrecognizedShutdownStrategyIsProvided_ItShouldPanic() {
	unknown := ShutdownStrategy(42)

	this.So(func() {
		NewSubscription("queue",
			SubscriptionOptions.AddWorkers(nil),
			SubscriptionOptions.ShutdownStrategy(unknown, 0))
	}, should.Panic)
}
func (this *SubscriptionConfigFixture) TestWhenShutdownStrategyIsImmediate_TimeoutIsSetToZero() {
	subscription := NewSubscription("queue",
		SubscriptionOptions.AddWorkers(nil),
		SubscriptionOptions.ShutdownStrategy(ShutdownStrategyImmediate, 42))

	this.So(subscription.shutdownStrategy, should.Equal, ShutdownStrategyImmediate)
	this.So(subscription.shutdownTimeout, should.Equal, 0)
}

func (this *SubscriptionConfigFixture) TestWhenNumberOfHandlersIsLargerThanBufferCapacity_BufferCapacitySetToNumberOfHandlers() {
	subscription := NewSubscription("queue",
		SubscriptionOptions.AddWorkers(nil, nil, nil, nil),
		SubscriptionOptions.BufferCapacity(2))

	this.So(subscription.bufferCapacity, should.Equal, len(subscription.handlers))
}

func (this *SubscriptionConfigFixture) TestWhenFullThrottle_MaximumValuesForBufferCapacityAndBatchCapacity() {
	subscription := NewSubscription("queue",
		SubscriptionOptions.AddWorkers(nil),
		SubscriptionOptions.FullThrottle())

	this.So(subscription.batchCapacity, should.Equal, 65535)
	this.So(subscription.bufferCapacity, should.Equal, 65535)
}
