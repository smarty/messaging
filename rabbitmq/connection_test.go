package rabbitmq

import (
	"context"
	"errors"
	"testing"

	"github.com/smarty/messaging/v3"
	"github.com/smarty/messaging/v3/rabbitmq/adapter"
	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/streadway/amqp"
)

func TestConnectionFixture(t *testing.T) {
	gunit.Run(new(ConnectionFixture), t)
}

type ConnectionFixture struct {
	*gunit.Fixture

	connection messaging.Connection

	txError      error
	channelError error
	closeError   error
	txCalls      int
}

func (this *ConnectionFixture) Setup() {
	this.connection = newConnection(this, configuration{Monitor: nop{}, Logger: nop{}})
}

func (this *ConnectionFixture) TestWhenOpeningReader_OpenAChannelAndReturnReader() {
	reader, err := this.connection.Reader(context.Background())

	this.So(reader, should.HaveSameTypeAs, &defaultReader{})
	this.So(err, should.BeNil)
}
func (this *ConnectionFixture) TestWhenOpeningReaderFails_ReturnUnderlyingError() {
	this.channelError = errors.New("")

	reader, err := this.connection.Reader(context.Background())

	this.So(reader, should.BeNil)
	this.So(err, should.Equal, this.channelError)
}

func (this *ConnectionFixture) TestWhenOpeningCommitWriter_OpenATransactionalChannelAndReturnWriter() {
	writer, err := this.connection.CommitWriter(context.Background())

	this.So(writer, should.HaveSameTypeAs, defaultWriter{})
	this.So(err, should.BeNil)
	this.So(this.txCalls, should.Equal, 1)
}
func (this *ConnectionFixture) TestWhenOpeningChannelForCommitWriterFails_ReturnUnderlyingError() {
	this.channelError = errors.New("")

	writer, err := this.connection.CommitWriter(context.Background())

	this.So(writer, should.BeNil)
	this.So(err, should.Equal, this.channelError)
	this.So(this.txCalls, should.Equal, 0)
}
func (this *ConnectionFixture) TestWhenMarkingChannelAsTransactionalFails_ReturnUnderlyingError() {
	this.txError = errors.New("")

	writer, err := this.connection.CommitWriter(context.Background())

	this.So(writer, should.BeNil)
	this.So(err, should.Equal, this.txError)
	this.So(this.txCalls, should.Equal, 1)
}

func (this *ConnectionFixture) TestWhenOpeningWriter_OpenATransactionalChannelAndReturnWriter() {
	writer, err := this.connection.Writer(context.Background())

	this.So(writer, should.HaveSameTypeAs, defaultWriter{})
	this.So(err, should.BeNil)
}
func (this *ConnectionFixture) TestWhenOpeningChannelForWriterFails_ReturnUnderlyingError() {
	this.channelError = errors.New("")

	writer, err := this.connection.Writer(context.Background())

	this.So(writer, should.BeNil)
	this.So(err, should.Equal, this.channelError)
}

func (this *ConnectionFixture) TestWhenClosing_InvokeUnderlyingConnection() {
	this.closeError = errors.New("")

	err := this.connection.Close()

	this.So(err, should.Equal, this.closeError)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *ConnectionFixture) Channel() (adapter.Channel, error) { return this, this.channelError }
func (this *ConnectionFixture) Close() error                      { return this.closeError }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *ConnectionFixture) Tx() error { this.txCalls++; return this.txError }

func (this *ConnectionFixture) DeclareQueue(name string, replicated bool) error   { panic("nop") }
func (this *ConnectionFixture) DeclareExchange(name string) error                 { panic("nop") }
func (this *ConnectionFixture) BindQueue(queue, exchange string) error            { panic("nop") }
func (this *ConnectionFixture) BufferCapacity(value uint16) error                 { panic("nop") }
func (this *ConnectionFixture) Consume(_, _ string) (<-chan amqp.Delivery, error) { panic("nop") }
func (this *ConnectionFixture) Ack(deliveryTag uint64, multiple bool) error       { panic("nop") }
func (this *ConnectionFixture) CancelConsumer(consumerID string) error            { panic("nop") }
func (this *ConnectionFixture) Publish(_, _ string, _ amqp.Publishing) error      { panic("nop") }
func (this *ConnectionFixture) TxCommit() error                                   { panic("nop") }
func (this *ConnectionFixture) TxRollback() error                                 { panic("nop") }
