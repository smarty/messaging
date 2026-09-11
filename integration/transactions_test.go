//go:build integration

package integration

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/smarty/gunit"
	"github.com/smarty/gunit/assert/should"
	"github.com/smarty/messaging/v4"
	"github.com/smarty/messaging/v4/rabbitmq"
)

func TestTransactionsFixture(t *testing.T) {
	gunit.Run(new(TransactionsFixture), t, gunit.Options.SequentialTestCases())
}

type TransactionsFixture struct {
	*gunit.Fixture

	ctx        context.Context
	shutdown   context.CancelFunc
	management *management
	log        capturingLog
	monitor    rabbitMonitor
	exchange   string
	queue      string
	transport  messaging.Connector
}

func (this *TransactionsFixture) Setup() {
	this.ctx, this.shutdown = context.WithTimeout(context.Background(), time.Second*30)
	this.management = newManagement()
	this.exchange = uniqueName("exchange")
	this.queue = uniqueName("queue")
	this.management.DeclareExchange(this.exchange)
	this.management.DeclareQueue(this.queue)
	this.management.Bind(this.queue, this.exchange)
	this.transport = rabbitmq.New(
		rabbitmq.Options.Address(brokerAddress),
		rabbitmq.Options.Logger(&this.log),
		rabbitmq.Options.Monitor(&this.monitor),
		rabbitmq.Options.BrokerTimeout(time.Second*5),
		rabbitmq.Options.PanicOnTopologyError(false), // so a 404 comes back as an error we can inspect
	)
}
func (this *TransactionsFixture) Teardown() {
	this.shutdown()
	_ = this.transport.Close()
	this.management.DeleteQueue(this.queue)
	this.management.DeleteExchange(this.exchange)
}

// The status probe's honesty (and the sever design) rests on this: a publish
// to a missing exchange is asynchronous, but the broker closes the channel
// with the reason and tx.commit returns it inside the same call.
func (this *TransactionsFixture) TestCommitAfterPublishingToAMissingExchange_ReturnsNotFoundFromCommit() {
	connection, err := this.transport.Connect(this.ctx)
	this.So(err, should.BeNil)
	defer func() { _ = connection.Close() }()
	writer, err := connection.CommitWriter(this.ctx)
	this.So(err, should.BeNil)

	count, writeErr := writer.Write(this.ctx, messaging.Dispatch{Topic: uniqueName("missing-exchange"), Payload: []byte("x")})
	commitErr := writer.Commit()

	this.So(count, should.Equal, 1)
	this.So(writeErr, should.BeNil) // asynchronous: the publish itself does not know
	var brokerErr *amqp.Error
	this.So(errors.As(commitErr, &brokerErr), should.BeTrue)
	this.So(brokerErr.Code, should.Equal, amqp.NotFound)
	this.So(this.monitor.Count("commit-failed"), should.Equal, 1)
}

func (this *TransactionsFixture) TestUnsupportedHeaderValue_IsRejectedWithoutKillingTheConnection() {
	connection, _ := this.transport.Connect(this.ctx)
	defer func() { _ = connection.Close() }()
	writer, _ := connection.CommitWriter(this.ctx)

	_, err := writer.Write(this.ctx, messaging.Dispatch{Topic: this.exchange, Payload: []byte("x"), Headers: map[string]any{"bad": uint64(1)}})

	this.So(errors.Is(err, rabbitmq.ErrInvalidHeader), should.BeTrue)
	// the same channel is still usable: nothing reached the wire
	_, err = writer.Write(this.ctx, messaging.Dispatch{Topic: this.exchange, MessageType: "ok", Payload: []byte("ok"), Headers: map[string]any{"fine": "yes"}})
	this.So(err, should.BeNil)
	this.So(writer.Commit(), should.BeNil)
	received, err := drain(this.ctx, this.transport, this.queue, time.Second)
	this.So(err, should.BeNil)
	this.So(received, should.Equal, []string{"ok"})
}

// Dispatch.Expiration is rendered in milliseconds. A message whose TTL has
// passed is discarded by the broker; one whose TTL has not is delivered.
func (this *TransactionsFixture) TestMessageExpiration_IsHonoredInMilliseconds() {
	err := publish(this.ctx, this.transport,
		messaging.Dispatch{Topic: this.exchange, MessageType: "long-lived", Payload: []byte("keep"), Expiration: time.Second * 30},
		messaging.Dispatch{Topic: this.exchange, MessageType: "short-lived", Payload: []byte("drop"), Expiration: time.Millisecond * 200},
	)
	this.So(err, should.BeNil)
	time.Sleep(time.Second) // let the short TTL pass; with the old seconds scale both would survive for 200s and 30 000s

	received, err := drain(this.ctx, this.transport, this.queue, time.Second*2)

	this.So(err, should.BeNil)
	this.So(received, should.Equal, []string{"long-lived"})
}

func (this *TransactionsFixture) TestQueueDeletedWhileConsuming_ReadReportsTheCancellation() {
	connection, _ := this.transport.Connect(this.ctx)
	defer func() { _ = connection.Close() }()
	reader, _ := connection.Reader(this.ctx)
	defer func() { _ = reader.Close() }()
	stream, err := reader.Stream(this.ctx, messaging.StreamConfig{StreamName: this.queue, BufferCapacity: 16})
	this.So(err, should.BeNil)
	defer func() { _ = stream.Close() }()
	result := make(chan error, 1)
	go func() { result <- stream.Read(this.ctx, &messaging.Delivery{}) }()

	this.management.DeleteQueue(this.queue)

	select {
	case err = <-result:
	case <-time.After(time.Second * 10):
		err = errors.New("Read did not return within 10s")
	}
	this.So(err, should.NotBeNil)
	this.So(errors.Is(err, io.EOF), should.BeTrue)
	this.So(err.Error(), should.ContainSubstring, "cancelled by the broker")
}
