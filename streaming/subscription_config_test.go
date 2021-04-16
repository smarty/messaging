package streaming

import (
	"testing"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/messaging/v3"
)

func TestSubscriptionConfigFixture(t *testing.T) {
	gunit.Run(new(SubscriptionConfigFixture), t)
}

type SubscriptionConfigFixture struct {
	*gunit.Fixture
}

func (this *SubscriptionConfigFixture) Setup() {
}

func (this *SubscriptionConfigFixture) TestWhenNoHandlersAreConfigured_ItShouldPanic() {
	this.So(func() { newSubscription() }, should.Panic)
}

func (this *SubscriptionConfigFixture) TestWhenValuesAreProvided_SubscriptionShouldHaveValues() {
	subscription := newSubscription(
		SubscriptionOptions.AddStream(
			StreamOptions.StreamName("queue"),
			StreamOptions.GroupName("group-name"),
			StreamOptions.Topics("topic1", "topic2"),
			StreamOptions.EstablishTopology(true),
			StreamOptions.Partition(6),
			StreamOptions.Sequence(7)),
		SubscriptionOptions.AddWorkers(nil),
		SubscriptionOptions.BatchCapacity(1),
		SubscriptionOptions.BufferCapacity(2),
		SubscriptionOptions.BufferDelayBetweenBatches(3),
		SubscriptionOptions.FullDeliveryToHandler(true),
		SubscriptionOptions.ReconnectDelay(5),
		SubscriptionOptions.ShutdownStrategy(ShutdownStrategyCurrentBatch, 4),
	)

	this.So(subscription, should.Resemble, Subscription{
		streamConfigs: []messaging.StreamConfig{
			{
				EstablishTopology: true,
				ExclusiveStream:   false,
				BufferCapacity:    1,
				MaxMessageBytes:   1024 * 1024,
				StreamName:        "queue",
				GroupName:         "group-name",
				Topics:            []string{"topic1", "topic2"},
				Partition:         6,
				Sequence:          7,
			},
		},
		handlers:         []messaging.Handler{nil},
		bufferCapacity:   2,
		batchCapacity:    1,
		handleDelivery:   true,
		bufferTimeout:    3,
		reconnectDelay:   5,
		shutdownStrategy: ShutdownStrategyCurrentBatch,
		shutdownTimeout:  4,
	})
}

func (this *SubscriptionConfigFixture) TestWhenUnrecognizedShutdownStrategyIsProvided_ItShouldPanic() {
	unknown := ShutdownStrategy(42)

	this.So(func() {
		newSubscription(
			SubscriptionOptions.AddWorkers(nil),
			SubscriptionOptions.ShutdownStrategy(unknown, 0))
	}, should.Panic)
}
func (this *SubscriptionConfigFixture) TestWhenShutdownStrategyIsImmediate_TimeoutIsSetToZero() {
	subscription := newSubscription(
		SubscriptionOptions.AddWorkers(nil),
		SubscriptionOptions.ShutdownStrategy(ShutdownStrategyImmediate, 42))

	this.So(subscription.shutdownStrategy, should.Equal, ShutdownStrategyImmediate)
	this.So(subscription.shutdownTimeout, should.Equal, 0)
}

func (this *SubscriptionConfigFixture) TestWhenNumberOfHandlersIsLargerThanBufferCapacity_BufferCapacitySetToNumberOfHandlers() {
	subscription := newSubscription(SubscriptionOptions.AddWorkers(nil, nil, nil, nil),
		SubscriptionOptions.BufferCapacity(2))

	this.So(subscription.bufferCapacity, should.Equal, len(subscription.handlers))
}

func (this *SubscriptionConfigFixture) TestWhenFullThrottle_MaximumValuesForBufferCapacityAndBatchCapacity() {
	subscription := newSubscription(SubscriptionOptions.AddWorkers(nil),
		SubscriptionOptions.FullThrottle())

	this.So(subscription.batchCapacity, should.Equal, 65535)
	this.So(subscription.bufferCapacity, should.Equal, 65535)
}
