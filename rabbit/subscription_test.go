package rabbit

import (
	"strconv"
	"testing"
	"time"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/clock"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/logging"
	"github.com/smartystreets/messaging"
	"github.com/streadway/amqp"
)

func TestSubscriptionFixture(t *testing.T) {
	gunit.Run(new(SubscriptionFixture), t)
}

type SubscriptionFixture struct {
	*gunit.Fixture

	queue        string
	bindings     []string
	channel      *FakeSubscriptionChannel
	subscription *Subscription

	control chan interface{}
	output  chan messaging.Delivery
}

func (this *SubscriptionFixture) Setup() {
	this.channel = newFakeSubscriptionChannel()
	this.control = make(chan interface{}, 4)
	this.output = make(chan messaging.Delivery, 8)
}
func (this *SubscriptionFixture) createSubscription() {
	this.subscription = newSubscription(
		this.channel, this.queue, this.bindings,
		this.control, this.output)
	this.subscription.logger = logging.Capture()
}

//////////////////////////////////////////////////////////////////

func (this *SubscriptionFixture) TestQueuedBasedSubscription() {
	this.queue = "test-queue"

	this.assertListen()

	this.So(this.channel.queue, should.Equal, this.queue)
	this.So(this.channel.exclusive, should.BeFalse)
}
func (this *SubscriptionFixture) TestExclusiveSubscription() {
	this.bindings = []string{"exchange1", "exchange2"}

	this.assertListen()

	this.So(this.channel.exclusive, should.BeTrue)
	this.So(this.channel.queue, should.NotBeEmpty)
	this.So(this.channel.boundQueue[0], should.Equal, this.channel.queue)
}
func (this *SubscriptionFixture) TestQueuedBasedWithBindingsSubscription() {
	this.queue = "test-queue"
	this.bindings = []string{"exchange1", "exchange2"}

	this.assertListen()

	this.So(this.channel.exclusive, should.BeFalse)
	this.So(this.channel.queue, should.Equal, this.queue)
	this.So(this.channel.boundQueue[0], should.Equal, this.channel.queue)
	this.So(this.channel.declaredExchange, should.Resemble, []string{"exchange1@fanout", "exchange2@fanout"})
}

func (this *SubscriptionFixture) TestFailingAMQPChannel() {
	this.queue = "test-queue"
	this.channel.incoming = nil

	this.assertListen()

	this.So(this.channel.queue, should.NotBeEmpty)
}
func (this *SubscriptionFixture) assertListen() {
	this.createSubscription()

	go this.subscription.Listen()
	this.channel.close()

	this.So((<-this.control).(subscriptionClosed).DeliveryCount, should.Equal, 0)
	this.So(this.channel.bufferSize, should.Equal, cap(this.output))
	this.So(this.channel.bindings, should.Resemble, this.bindings)
	this.So(this.channel.consumer, should.NotBeEmpty)
}

//////////////////////////////////////////////////////////////////

func (this *SubscriptionFixture) TestDeliveriesArePushedToTheApplication() {
	this.queue = "test-queue"
	delivery1 := amqp.Delivery{Type: "test-message", Body: []byte{1, 2, 3, 4, 5}, DeliveryTag: 17}
	delivery2 := amqp.Delivery{Type: "test-message2", Body: []byte{6, 7, 8, 9, 10}, DeliveryTag: 18}

	this.channel.incoming <- delivery1
	this.channel.incoming <- delivery2
	close(this.channel.incoming)

	this.createSubscription()
	go this.subscription.Listen()

	this.So((<-this.output), should.Resemble, messaging.Delivery{
		MessageType: "test-message",
		Payload:     []byte{1, 2, 3, 4, 5},
		Receipt:     newReceipt(this.channel, delivery1.DeliveryTag),
		Upstream:    delivery1,
	})
	this.So((<-this.output), should.Resemble, messaging.Delivery{
		MessageType: "test-message2",
		Payload:     []byte{6, 7, 8, 9, 10},
		Receipt:     newReceipt(this.channel, delivery2.DeliveryTag),
		Upstream:    delivery2,
	})

	message := (<-this.control).(subscriptionClosed)
	this.So(message.DeliveryCount, should.Equal, 2)
	this.So(message.LatestConsumer, should.Equal, this.channel)
	this.So(message.LatestDeliveryTag, should.Equal, delivery2.DeliveryTag)
}

//////////////////////////////////////////////////////////////////

func (this *SubscriptionFixture) TestConsumerCancellation() {
	this.createSubscription()
	this.subscription.Close()
	this.So(this.channel.cancelled, should.BeTrue)
	this.So(this.channel.consumer, should.NotBeEmpty)
}

//////////////////////////////////////////////////////////////////

type FakeSubscriptionChannel struct {
	bufferSize       int
	queue            string
	consumer         string
	declaredExchange []string
	boundQueue       []string
	bindings         []string
	exclusive        bool
	cancelled        bool
	incoming         chan amqp.Delivery
}

func newFakeSubscriptionChannel() *FakeSubscriptionChannel {
	return &FakeSubscriptionChannel{
		incoming: make(chan amqp.Delivery, 16),
	}
}

func (this *FakeSubscriptionChannel) ConfigureChannelBuffer(value int) error {
	this.bufferSize = value
	return nil
}
func (this *FakeSubscriptionChannel) DeclareExchange(name, kind string) error {
	this.declaredExchange = append(this.declaredExchange, name+"@"+kind)
	return nil
}
func (this *FakeSubscriptionChannel) DeclareQueue(name string) error {
	this.boundQueue = append(this.boundQueue, name)
	return nil
}
func (this *FakeSubscriptionChannel) DeclareTransientQueue() (string, error) {
	return strconv.FormatInt(clock.UTCNow().UnixNano(), 10), nil
}
func (this *FakeSubscriptionChannel) BindExchangeToQueue(queue string, exchange string) error {
	this.boundQueue = append(this.boundQueue, queue)
	this.bindings = append(this.bindings, exchange)
	return nil
}

func (this *FakeSubscriptionChannel) Consume(queue, consumer string) (<-chan amqp.Delivery, error) {
	this.queue = queue
	this.consumer = consumer
	this.exclusive = false
	return this.incoming, nil
}
func (this *FakeSubscriptionChannel) ExclusiveConsume(queue string, consumer string) (<-chan amqp.Delivery, error) {
	this.queue = queue
	this.consumer = consumer
	this.exclusive = true
	return this.incoming, nil
}
func (this *FakeSubscriptionChannel) ConsumeWithoutAcknowledgement(string, string) (<-chan amqp.Delivery, error) {
	return nil, nil
}
func (this *FakeSubscriptionChannel) ExclusiveConsumeWithoutAcknowledgement(string, string) (<-chan amqp.Delivery, error) {
	return nil, nil
}
func (this *FakeSubscriptionChannel) CancelConsumer(consumer string) error {
	this.cancelled = true
	this.consumer = consumer
	return nil
}

func (this *FakeSubscriptionChannel) AcknowledgeSingleMessage(uint64) error    { return nil }
func (this *FakeSubscriptionChannel) AcknowledgeMultipleMessages(uint64) error { return nil }
func (this *FakeSubscriptionChannel) Close() error                             { return nil }

func (this *FakeSubscriptionChannel) close() {
	time.Sleep(time.Millisecond)
	if this.incoming != nil {
		close(this.incoming)
	}
}
