package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/messaging/v4"
	"github.com/streadway/amqp"
)

func TestWriterFixture(t *testing.T) {
	gunit.Run(new(WriterFixture), t)
}

type WriterFixture struct {
	*gunit.Fixture

	writer                 messaging.CommitWriter
	now                    time.Time
	panicOnTopologyFailure bool

	closeError              error
	commitError             error
	rollbackError           error
	publishError            error
	publishCallsBeforeError int

	publishExchanges []string
	publishKeys      []string
	publishMessages  []amqp.Publishing
}

func (this *WriterFixture) Setup() {
	this.initializeWriter()
}
func (this *WriterFixture) initializeWriter() {
	config := configuration{}
	Options.apply(
		Options.Now(func() time.Time { return this.now }),
		Options.PanicOnTopologyError(this.panicOnTopologyFailure),
	)(&config)

	this.writer = newWriter(this, config)
}

func (this *WriterFixture) TestWhenCloseInvoked_UnderlyingChannelClosed() {
	this.closeError = errors.New("")

	err := this.writer.Close()

	this.So(err, should.Equal, this.closeError)
}
func (this *WriterFixture) TestWhenUnderlyingRollbackFails_ReturnError() {
	this.rollbackError = errors.New("")

	err := this.writer.Rollback()

	this.So(err, should.Equal, this.rollbackError)
}
func (this *WriterFixture) TestUnderlyingRollbackSucceeds_ReturnNil() {
	err := this.writer.Rollback()

	this.So(err, should.BeNil)
}
func (this *WriterFixture) TestWhenUnderlyingCommitFails_ReturnError() {
	this.commitError = errors.New("")

	err := this.writer.Commit()

	this.So(err, should.Equal, this.commitError)
}
func (this *WriterFixture) TestWhenUnderlyingCommitSucceeds_ReturnNil() {
	err := this.writer.Commit()

	this.So(err, should.BeNil)
}
func (this *WriterFixture) TestWhenTopologyNotEstablished_PanicOnTopologyErrors() {
	this.panicOnTopologyFailure = true
	this.initializeWriter()

	this.commitError = &amqp.Error{Code: 404}

	this.So(func() { _ = this.writer.Commit() }, should.Panic)
}
func (this *WriterFixture) TestWhenTopologyNotEstablished_DontPanicIfIsNotTopologyRelated() {
	this.panicOnTopologyFailure = true
	this.initializeWriter()

	this.commitError = errors.New("")

	err := this.writer.Commit()

	this.So(err, should.Equal, this.commitError)
}

func (this *WriterFixture) TestWhenWrite_TopicMissing() {
	count, err := this.writer.Write(context.Background(), messaging.Dispatch{})

	this.So(err, should.Equal, messaging.ErrEmptyDispatchTopic)
	this.So(count, should.Equal, 0)

	this.So(this.publishExchanges, should.BeEmpty)
	this.So(this.publishKeys, should.BeEmpty)
	this.So(this.publishMessages, should.BeEmpty)
}
func (this *WriterFixture) TestWhenWrite_PublishToUnderlyingChannel() {
	count, err := this.writer.Write(context.Background(), messaging.Dispatch{
		SourceID:        1,
		MessageID:       2,
		CorrelationID:   3,
		Timestamp:       time.Time{},
		Expiration:      time.Minute,
		Durable:         true,
		Topic:           "topic",
		Partition:       5,
		MessageType:     "message-type",
		ContentType:     "content-type",
		ContentEncoding: "content-encoding",
		Payload:         []byte("payload"),
		Headers: map[string]interface{}{
			"header10": "value10",
			"header20": int64(20),
			"header30": false,
		},
	})

	this.So(err, should.BeNil)
	this.So(count, should.Equal, 1)

	this.So(this.publishExchanges, should.Resemble, []string{"topic"})
	this.So(this.publishKeys, should.Resemble, []string{"5"})
	this.So(this.publishMessages, should.Resemble, []amqp.Publishing{
		{
			ContentType:     "content-type",
			ContentEncoding: "content-encoding",
			DeliveryMode:    amqp.Persistent,
			Priority:        0,
			CorrelationId:   "3",
			ReplyTo:         "",
			Expiration:      "60",
			MessageId:       "2",
			Timestamp:       time.Time{},
			Type:            "message-type",
			UserId:          "",
			AppId:           "1",
			Body:            []byte("payload"),
			Headers: map[string]interface{}{
				"header10": "value10",
				"header20": int64(20),
				"header30": false,
			},
		},
	})
}
func (this *WriterFixture) TestWhenWriteTransientMessage_PublishTransientMessageToUnderlyingChannel() {
	const durable = false

	count, err := this.writer.Write(context.Background(), messaging.Dispatch{
		Topic:   "a",
		Durable: durable,
	})

	this.So(err, should.BeNil)
	this.So(count, should.Equal, 1)

	this.So(this.publishExchanges, should.Resemble, []string{"a"})
	this.So(this.publishKeys, should.Resemble, []string{"0"})
	this.So(this.publishMessages, should.Resemble, []amqp.Publishing{
		{

			MessageId:     "0",
			CorrelationId: "0",
			AppId:         "0",
			DeliveryMode:  amqp.Transient,
		},
	})
}
func (this *WriterFixture) TestWhenWriteExpirationLessThanOneSecond_UseOneSecondExpiration() {
	count, err := this.writer.Write(context.Background(), messaging.Dispatch{
		Topic:      "a",
		Expiration: time.Second - 1,
	})

	this.So(err, should.BeNil)
	this.So(count, should.Equal, 1)

	this.So(this.publishExchanges, should.Resemble, []string{"a"})
	this.So(this.publishKeys, should.Resemble, []string{"0"})
	this.So(this.publishMessages, should.Resemble, []amqp.Publishing{
		{
			MessageId:     "0",
			CorrelationId: "0",
			AppId:         "0",
			DeliveryMode:  amqp.Transient,
			Expiration:    "1",
		},
	})
}
func (this *WriterFixture) TestWhenWriterFailsMidwayThrough_ReturnNumberOfWritesThusFarAndError() {
	this.publishError = errors.New("")
	this.publishCallsBeforeError = 3

	count, err := this.writer.Write(context.Background(), []messaging.Dispatch{{Topic: "a"}, {Topic: "a"}, {Topic: "a"}}...)

	this.So(count, should.Equal, 2)
	this.So(err, should.Equal, this.publishError)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *WriterFixture) Close() error      { return this.closeError }
func (this *WriterFixture) TxCommit() error   { return this.commitError }
func (this *WriterFixture) TxRollback() error { return this.rollbackError }
func (this *WriterFixture) Publish(exchange, key string, envelope amqp.Publishing) error {
	this.publishExchanges = append(this.publishExchanges, exchange)
	this.publishKeys = append(this.publishKeys, key)
	this.publishMessages = append(this.publishMessages, envelope)

	if len(this.publishMessages) >= this.publishCallsBeforeError {
		return this.publishError
	}

	return nil
}

func (this *WriterFixture) DeclareQueue(name string) error {
	panic("nop")
}
func (this *WriterFixture) DeclareExchange(name string) error {
	panic("nop")
}
func (this *WriterFixture) BindQueue(queue, exchange string) error {
	panic("nop")
}
func (this *WriterFixture) BufferCapacity(value uint16) error {
	panic("nop")
}
func (this *WriterFixture) Consume(consumerID, queue string) (<-chan amqp.Delivery, error) {
	panic("nop")
}
func (this *WriterFixture) Ack(deliveryTag uint64, multiple bool) error {
	panic("nop")
}
func (this *WriterFixture) CancelConsumer(consumerID string) error {
	panic("nop")
}
func (this *WriterFixture) Tx() error {
	panic("nop")
}
