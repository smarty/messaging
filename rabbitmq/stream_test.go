package rabbitmq

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/messaging/v3"
	"github.com/streadway/amqp"
)

func TestStreamFixture(t *testing.T) {
	gunit.Run(new(StreamFixture), t)
}

type StreamFixture struct {
	*gunit.Fixture

	stream          messaging.Stream
	deliveries      chan amqp.Delivery
	streamID        string
	streamName      string
	exclusiveStream bool
	now             time.Time

	cancellations         []string
	acknowledgedTags      []uint64
	acknowledgedMultiples []bool
	acknowledgeError      error
}

func (this *StreamFixture) Setup() {
	this.now = time.Now().UTC()
	this.deliveries = make(chan amqp.Delivery, 16)
	this.initializeStream()
}
func (this *StreamFixture) initializeStream() {
	this.streamID = "streamID"
	this.streamName = "streamName"
	this.stream = newStream(this, this.deliveries, this.streamID, this.streamName, this.exclusiveStream,
		configuration{Logger: nop{}, Monitor: nop{}})
}

func (this *StreamFixture) TestWhenCloseInvokedMultipleTimes_OnlyCancelConsumerOnce() {
	_ = this.stream.Close()

	_ = this.stream.Close()

	this.So(this.cancellations, should.Resemble, []string{this.streamID})
}

func (this *StreamFixture) TestWhenReading_ReadFromConsumerBufferChannel() {
	raw := amqp.Delivery{
		ContentType:     "content-type",
		ContentEncoding: "content-encoding",
		DeliveryMode:    amqp.Persistent,
		CorrelationId:   "1",
		Expiration:      "2",
		MessageId:       "3",
		Timestamp:       this.now,
		Type:            "message-type",
		UserId:          "4",
		AppId:           "5",
		DeliveryTag:     6,
		Redelivered:     false,
		Body:            []byte("payload"),
		Headers: map[string]any{
			"header10": "value10",
			"header20": int64(20),
			"header30": false,
		},
	}
	this.deliveries <- raw

	var delivery messaging.Delivery
	err := this.stream.Read(context.Background(), &delivery)

	this.So(err, should.BeNil)
	this.So(delivery, should.Resemble, messaging.Delivery{
		Upstream:        raw,
		DeliveryID:      6,
		SourceID:        5,
		MessageID:       3,
		CorrelationID:   1,
		Timestamp:       this.now,
		Durable:         true,
		Topic:           "streamName",
		MessageType:     "message-type",
		ContentType:     "content-type",
		ContentEncoding: "content-encoding",
		Payload:         []byte("payload"),
		Headers: map[string]any{
			"header10": "value10",
			"header20": int64(20),
			"header30": false,
		},
	})
}
func (this *StreamFixture) TestWhenReadingFromAClosedBufferChannel_ReturnEOF() {
	close(this.deliveries)

	var delivery messaging.Delivery
	err := this.stream.Read(context.Background(), &delivery)

	this.So(err, should.Equal, io.EOF)
	this.So(delivery, should.Resemble, messaging.Delivery{})
}
func (this *StreamFixture) TestWhenContextIsCancelled_ReturnCancellationError() {
	dead, shutdown := context.WithCancel(context.Background())
	shutdown()

	var delivery messaging.Delivery
	err := this.stream.Read(dead, &delivery)

	this.So(err, should.Equal, context.Canceled)
	this.So(delivery, should.Resemble, messaging.Delivery{})
}

func (this *StreamFixture) TestWhenAcknowledgingADeliveryWithACancelledContext_ReturnError() {
	dead, shutdown := context.WithCancel(context.Background())
	shutdown()

	err := this.stream.Acknowledge(dead, messaging.Delivery{})

	this.So(err, should.Equal, context.Canceled)
	this.So(this.acknowledgedTags, should.BeEmpty)
}
func (this *StreamFixture) TestWhenAcknowledgingManyDeliveriesOnExclusiveStream_OnlyAckLastOne() {
	this.exclusiveStream = true
	this.initializeStream()

	err := this.stream.Acknowledge(context.Background(),
		messaging.Delivery{DeliveryID: 1},
		messaging.Delivery{DeliveryID: 2},
		messaging.Delivery{DeliveryID: 3},
	)

	this.So(err, should.BeNil)
	this.So(this.acknowledgedTags, should.Resemble, []uint64{3})
	this.So(this.acknowledgedMultiples, should.Resemble, []bool{true})
}
func (this *StreamFixture) TestWhenAcknowledgingManyDeliveriesOnSharedStream_EachEachDelivery() {
	err := this.stream.Acknowledge(context.Background(),
		messaging.Delivery{DeliveryID: 1},
		messaging.Delivery{DeliveryID: 2},
		messaging.Delivery{DeliveryID: 3},
	)

	this.So(err, should.BeNil)
	this.So(this.acknowledgedTags, should.Resemble, []uint64{1, 2, 3})
	this.So(this.acknowledgedMultiples, should.Resemble, []bool{false, false, false})
}
func (this *StreamFixture) TestWhenAcknowledgingFails_ReturnUnderlyingError() {
	this.acknowledgeError = errors.New("")

	err := this.stream.Acknowledge(context.Background(), messaging.Delivery{DeliveryID: 1})

	this.So(err, should.Equal, this.acknowledgeError)
	this.So(this.acknowledgedTags, should.Resemble, []uint64{1})
	this.So(this.acknowledgedMultiples, should.Resemble, []bool{false})
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *StreamFixture) CancelConsumer(consumerID string) error {
	this.cancellations = append(this.cancellations, consumerID)
	return nil
}
func (this *StreamFixture) Ack(deliveryTag uint64, multiple bool) error {
	this.acknowledgedTags = append(this.acknowledgedTags, deliveryTag)
	this.acknowledgedMultiples = append(this.acknowledgedMultiples, multiple)
	return this.acknowledgeError
}

func (this *StreamFixture) DeclareQueue(name string, replicated bool) error   { panic("nop") }
func (this *StreamFixture) DeclareExchange(name string) error                 { panic("nop") }
func (this *StreamFixture) BindQueue(queue, exchange string) error            { panic("nop") }
func (this *StreamFixture) BufferCapacity(value uint16) error                 { panic("nop") }
func (this *StreamFixture) Consume(_, _ string) (<-chan amqp.Delivery, error) { panic("nop") }
func (this *StreamFixture) Publish(_, _ string, _ amqp.Publishing) error      { panic("nop") }
func (this *StreamFixture) Tx() error                                         { panic("nop") }
func (this *StreamFixture) TxCommit() error                                   { panic("nop") }
func (this *StreamFixture) TxRollback() error                                 { panic("nop") }
func (this *StreamFixture) Close() error                                      { panic("nop") }
