package rabbitmq

import (
	"context"
	"errors"
	"testing"

	"github.com/smarty/assertions/should"
	"github.com/smarty/gunit"
	"github.com/smarty/messaging/v3"
	"github.com/streadway/amqp"
)

func TestReaderFixture(t *testing.T) {
	gunit.Run(new(ReaderFixture), t)
}

type ReaderFixture struct {
	*gunit.Fixture

	configPanicOnTopologyFailure bool

	reader messaging.Reader

	declareQueueName       string
	declareQueueError      error
	declareExchangeNames   []string
	declareExchangeError   error
	bindQueueQueueNames    []string
	bindQueueExchangeNames []string
	bindQueueError         error
	bufferCapacityValue    uint16
	bufferCapacityError    error
	consumeConsumerID      string
	consumeQueue           string
	consumeChannel         chan amqp.Delivery
	consumeError           error
	callsToClose           int
	cancelledConsumers     []string
}

func (this *ReaderFixture) Setup() {
	this.consumeChannel = make(chan amqp.Delivery, 4)
	this.initializeReader()
}
func (this *ReaderFixture) initializeReader() {
	config := configuration{}
	Options.apply(Options.PanicOnTopologyError(this.configPanicOnTopologyFailure))(&config)
	this.reader = newReader(this, config)
}

func (this *ReaderFixture) TestWhenEstablishingAStream_StartConsumerOnFromUnderlyingChannel() {
	stream, err := this.reader.Stream(context.Background(), messaging.StreamConfig{
		EstablishTopology: true,
		ExclusiveStream:   true,
		BufferCapacity:    2,
		StreamName:        "queue",
		Topics:            []string{"topic1", "topic2"},
	})

	this.So(stream, should.HaveSameTypeAs, &defaultStream{})
	this.So(err, should.BeNil)

	this.So(this.declareQueueName, should.Equal, "queue")
	this.So(this.declareExchangeNames, should.Resemble, []string{"topic1", "topic2"})
	this.So(this.bindQueueQueueNames, should.Resemble, []string{"queue", "queue"})
	this.So(this.bindQueueExchangeNames, should.Resemble, []string{"topic1", "topic2"})
	this.So(this.bufferCapacityValue, should.Equal, 2)
	this.So(this.consumeConsumerID, should.Equal, "0")
	this.So(this.consumeQueue, should.Equal, "queue")
}
func (this *ReaderFixture) TestWhenEstablishingAnExclusiveStreamWithExisting_ReturnError() {
	_, _ = this.reader.Stream(context.Background(), messaging.StreamConfig{}) // stream already exists

	stream, err := this.reader.Stream(context.Background(), messaging.StreamConfig{
		EstablishTopology: true,
		ExclusiveStream:   true,
		BufferCapacity:    2,
		StreamName:        "queue",
		Topics:            []string{"topic1", "topic2"},
	})

	this.So(stream, should.BeNil)
	this.So(err, should.Equal, ErrMultipleStreams)
}
func (this *ReaderFixture) TestWhenExclusiveStreamAlreadyEstablishedAndSecondStreamDesired_ReturnError() {
	_, _ = this.reader.Stream(context.Background(), messaging.StreamConfig{ExclusiveStream: true})

	stream, err := this.reader.Stream(context.Background(), messaging.StreamConfig{ExclusiveStream: false})

	this.So(stream, should.BeNil)
	this.So(err, should.Equal, ErrAlreadyExclusive)
}

func (this *ReaderFixture) TestWhenEstablishingTopologyDeclareQueueFails_CloseChannelAndReturnError() {
	this.declareQueueError = errors.New("")
	config := messaging.StreamConfig{EstablishTopology: true}

	stream, err := this.reader.Stream(context.Background(), config)

	this.So(stream, should.BeNil)
	this.So(err, should.Equal, this.declareQueueError)
	this.So(this.callsToClose, should.Equal, 1)
}
func (this *ReaderFixture) TestWhenEstablishingTopologyDeclareExchangeFails_CloseChannelAndReturnError() {
	this.declareExchangeError = errors.New("")
	config := messaging.StreamConfig{EstablishTopology: true, StreamName: "queue", Topics: []string{"exchange"}}

	stream, err := this.reader.Stream(context.Background(), config)

	this.So(stream, should.BeNil)
	this.So(err, should.Equal, this.declareExchangeError)
	this.So(this.callsToClose, should.Equal, 1)
}
func (this *ReaderFixture) TestWhenEstablishingTopologyBindQueueFails_CloseChannelAndReturnError() {
	this.bindQueueError = errors.New("")
	config := messaging.StreamConfig{EstablishTopology: true, StreamName: "queue", Topics: []string{"exchange"}}

	stream, err := this.reader.Stream(context.Background(), config)

	this.So(stream, should.BeNil)
	this.So(err, should.Equal, this.bindQueueError)
	this.So(this.callsToClose, should.Equal, 1)
}
func (this *ReaderFixture) TestWhenTopologyRedeclarationConflictOccurs_CloseChannelAndPanicWhenConfigured() {
	this.configPanicOnTopologyFailure = true
	this.initializeReader()
	this.declareQueueError = &amqp.Error{Code: 406}
	config := messaging.StreamConfig{EstablishTopology: true}

	this.So(func() { _, _ = this.reader.Stream(context.Background(), config) }, should.Panic)
	this.So(this.callsToClose, should.Equal, 1)
}
func (this *ReaderFixture) TestWhenTopologyRedeclarationConflictOccurs_CloseChannelAndDontPanicWhenNotTopologyError() {
	this.configPanicOnTopologyFailure = true
	this.initializeReader()
	this.declareQueueError = errors.New("regular error, not topology related")
	config := messaging.StreamConfig{EstablishTopology: true}

	stream, err := this.reader.Stream(context.Background(), config)

	this.So(stream, should.BeNil)
	this.So(err, should.Equal, this.declareQueueError)
	this.So(this.callsToClose, should.Equal, 1)
}

func (this *ReaderFixture) TestWhenTopologyAvailableTopicsDeclared_DeclareAllOfThem() {
	stream, err := this.reader.Stream(context.Background(), messaging.StreamConfig{
		EstablishTopology: true,
		ExclusiveStream:   true,
		BufferCapacity:    2,
		StreamName:        "queue",
		Topics:            []string{"subscribed"},
		AvailableTopics:   []string{"another-topic-1", "another-topic-2"},
	})

	this.So(stream, should.HaveSameTypeAs, &defaultStream{})
	this.So(err, should.BeNil)

	this.So(this.declareQueueName, should.Equal, "queue")
	this.So(this.declareExchangeNames, should.Resemble, []string{"subscribed", "another-topic-1", "another-topic-2"})
	this.So(this.bindQueueQueueNames, should.Resemble, []string{"queue"})
	this.So(this.bindQueueExchangeNames, should.Resemble, []string{"subscribed"})
	this.So(this.bufferCapacityValue, should.Equal, 2)
	this.So(this.consumeConsumerID, should.Equal, "0")
	this.So(this.consumeQueue, should.Equal, "queue")
}

func (this *ReaderFixture) TestWhenSettingBufferCapacityFails_CloseChannelAndReturnError() {
	this.bufferCapacityError = errors.New("")

	stream, err := this.reader.Stream(context.Background(), messaging.StreamConfig{})

	this.So(stream, should.BeNil)
	this.So(err, should.Equal, this.bufferCapacityError)
}
func (this *ReaderFixture) TestWhenStartingConsumerFails_CloseChannelAndReturnError() {
	this.consumeError = errors.New("")

	stream, err := this.reader.Stream(context.Background(), messaging.StreamConfig{})

	this.So(stream, should.BeNil)
	this.So(err, should.Equal, this.consumeError)
}

func (this *ReaderFixture) TestWhenClosing_ShutDownAllStreamsAndUnderlyingChannel() {
	_, _ = this.reader.Stream(context.Background(), messaging.StreamConfig{})
	_, _ = this.reader.Stream(context.Background(), messaging.StreamConfig{})
	_, _ = this.reader.Stream(context.Background(), messaging.StreamConfig{})

	_ = this.reader.Close()

	this.So(this.cancelledConsumers, should.Resemble, []string{"0", "1", "2"})
	this.So(this.callsToClose, should.Equal, 1)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *ReaderFixture) DeclareQueue(name string, replicated bool) error {
	this.declareQueueName = name
	return this.declareQueueError
}
func (this *ReaderFixture) DeclareExchange(name string) error {
	this.declareExchangeNames = append(this.declareExchangeNames, name)
	return this.declareExchangeError
}
func (this *ReaderFixture) BindQueue(queue, exchange string) error {
	this.bindQueueQueueNames = append(this.bindQueueQueueNames, queue)
	this.bindQueueExchangeNames = append(this.bindQueueExchangeNames, exchange)
	return this.bindQueueError
}
func (this *ReaderFixture) BufferCapacity(value uint16) error {
	this.bufferCapacityValue = value
	return this.bufferCapacityError
}
func (this *ReaderFixture) Consume(consumerID, queue string) (<-chan amqp.Delivery, error) {
	this.consumeConsumerID = consumerID
	this.consumeQueue = queue
	return this.consumeChannel, this.consumeError
}
func (this *ReaderFixture) Close() error { this.callsToClose++; return nil }

func (this *ReaderFixture) Ack(deliveryTag uint64, multiple bool) error {
	panic("nop")
}
func (this *ReaderFixture) CancelConsumer(consumerID string) error {
	this.cancelledConsumers = append(this.cancelledConsumers, consumerID)
	return nil
}
func (this *ReaderFixture) Publish(exchange, key string, envelope amqp.Publishing) error {
	panic("nop")
}
func (this *ReaderFixture) Tx() error {
	panic("nop")
}
func (this *ReaderFixture) TxCommit() error {
	panic("nop")
}
func (this *ReaderFixture) TxRollback() error {
	panic("nop")
}
