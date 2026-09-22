//go:build integration

package integration

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/smarty/gunit"
	"github.com/smarty/gunit/assert/should"
	"github.com/smarty/messaging/v4"
	"github.com/smarty/messaging/v4/rabbitmq"
	"github.com/smarty/messaging/v4/serialization"
	"github.com/smarty/messaging/v4/streaming"
)

func TestPublishConsumeFixture(t *testing.T) {
	gunit.Run(new(PublishConsumeFixture), t, gunit.Options.SequentialTestCases())
}

type OrderPlaced struct {
	OrderID uint64 `json:"order_id"`
}

type PublishConsumeFixture struct {
	*gunit.Fixture

	ctx        context.Context
	shutdown   context.CancelFunc
	management *management
	log        capturingLog
	monitor    rabbitMonitor
	exchange   string
	queue      string
	transport  messaging.Connector
	encoded    messaging.Connector
}

func (this *PublishConsumeFixture) Setup() {
	this.ctx, this.shutdown = context.WithTimeout(context.Background(), time.Second*30)
	this.management = newManagement()
	this.exchange = uniqueName("exchange")
	this.queue = uniqueName("queue")
	this.transport = rabbitmq.New(
		rabbitmq.Options.Address(brokerAddress),
		rabbitmq.Options.Logger(&this.log),
		rabbitmq.Options.Monitor(&this.monitor),
		rabbitmq.Options.BrokerTimeout(time.Second*5),
	)
	this.encoded = serialization.New(this.transport,
		serialization.Options.WriteTypes(map[reflect.Type]string{reflect.TypeOf(OrderPlaced{}): this.exchange}),
		serialization.Options.ReadTypes(map[string]reflect.Type{this.exchange: reflect.TypeOf(OrderPlaced{})}),
		serialization.Options.Logger(&this.log),
	)
}
func (this *PublishConsumeFixture) Teardown() {
	this.shutdown()
	_ = this.transport.Close()
	this.management.DeleteQueue(this.queue)
	this.management.DeleteExchange(this.exchange)
}

func (this *PublishConsumeFixture) TestMessagesPublishedInATransaction_AreConsumedAndAcknowledged() {
	handler := &collectingHandler{}
	consumer := streaming.New(this.encoded,
		streaming.Options.Logger(&this.log),
		streaming.Options.Subscriptions(streaming.NewSubscription(this.queue,
			streaming.SubscriptionOptions.Topics(this.exchange),
			streaming.SubscriptionOptions.AddWorkers(handler),
			streaming.SubscriptionOptions.BatchCapacity(16),
		)),
	)
	consumerDone := make(chan struct{})
	go func() { defer close(consumerDone); consumer.Listen() }()
	this.So(waitFor(time.Second*10, func() bool { return this.management.QueueDepth(this.queue) >= 0 }), should.BeTrue) // topology established

	err := publish(this.ctx, this.encoded,
		messaging.Dispatch{Message: OrderPlaced{OrderID: 1}},
		messaging.Dispatch{Message: OrderPlaced{OrderID: 2}},
		messaging.Dispatch{Message: OrderPlaced{OrderID: 3}},
	)
	this.So(err, should.BeNil)

	this.So(waitFor(time.Second*10, func() bool { return len(handler.Messages()) == 3 }), should.BeTrue)
	this.So(handler.Messages(), should.Equal, []any{OrderPlaced{OrderID: 1}, OrderPlaced{OrderID: 2}, OrderPlaced{OrderID: 3}})
	this.So(this.monitor.Count("committed"), should.Equal, 1)
	this.So(this.monitor.Count("published"), should.Equal, 3)
	this.So(waitFor(time.Second*10, func() bool { return this.monitor.Count("acknowledged") >= 1 }), should.BeTrue)

	_ = consumer.Close()
	<-consumerDone
	leftover, err := drain(this.ctx, this.transport, this.queue, time.Second)
	this.So(err, should.BeNil)
	this.So(leftover, should.BeEmpty) // acknowledged, not requeued
}
