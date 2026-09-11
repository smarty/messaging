package adapter

import (
	"context"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type amqpConnector struct{}

// Connect completes the AMQP handshake on an already-dialed socket. amqp.Open
// sets no deadline of its own, so the caller's context deadline is applied to
// the socket for the duration of the handshake; a peer that accepts TCP and
// then hangs (an auth backend that never answers) fails instead of parking
// the caller forever. amqp091 clears the deadline itself once the handshake
// completes.
func (this amqpConnector) Connect(ctx context.Context, socket net.Conn, config Config) (Connection, error) {
	plainAuth := &amqp.PlainAuth{Username: config.Username, Password: config.Password}
	amqpConfig := amqp.Config{
		SASL:      []amqp.Authentication{plainAuth},
		Vhost:     config.VirtualHost,
		Heartbeat: config.Heartbeat,
	}

	if deadline, ok := ctx.Deadline(); ok {
		_ = socket.SetDeadline(deadline)
	}

	if connection, err := amqp.Open(socket, amqpConfig); err != nil {
		return nil, err
	} else {
		// registered here, immediately after the handshake, so a broker that
		// blocks or closes the connection right away is not missed
		blocked := connection.NotifyBlocked(make(chan amqp.Blocking, 1))
		closes := connection.NotifyClose(make(chan *amqp.Error, 1))
		return amqpConnection{Connection: connection, blocked: blocked, closes: closes}, nil
	}
}

type amqpConnection struct {
	*amqp.Connection
	blocked chan amqp.Blocking
	closes  chan *amqp.Error
}

func (this amqpConnection) BlockedNotifications() <-chan amqp.Blocking { return this.blocked }
func (this amqpConnection) CloseNotifications() <-chan *amqp.Error     { return this.closes }

// Close bounds the close handshake with a deadline on the underlying socket.
// Setting the deadline also unblocks any write already stalled on a broker
// that has stopped reading (a resource alarm), so Close cannot hang.
func (this amqpConnection) Close() error {
	return this.Connection.CloseDeadline(time.Now().Add(closeGracePeriod))
}

const closeGracePeriod = time.Second * 5

func (this amqpConnection) Channel() (Channel, error) {
	if channel, err := this.Connection.Channel(); err != nil {
		return nil, err
	} else {
		// registered at open, before any consumer or publish, so no close or
		// cancel can slip through unobserved
		return amqpChannel{
			Channel: channel,
			closes:  channel.NotifyClose(make(chan *amqp.Error, 1)),
			cancels: channel.NotifyCancel(make(chan string, 1)),
		}, nil
	}
}

type amqpChannel struct {
	*amqp.Channel
	closes  chan *amqp.Error
	cancels chan string
}

func (this amqpChannel) CloseNotifications() <-chan *amqp.Error { return this.closes }
func (this amqpChannel) CancelNotifications() <-chan string     { return this.cancels }

func (this amqpChannel) DeclareQueue(name string, replicated bool) error {
	if replicated {
		return this.declareQueue(name, "quorum")
	} else {
		return this.declareQueue(name, "classic")
	}
}
func (this amqpChannel) declareQueue(name, style string) error {
	_, err := this.Channel.QueueDeclare(name, true, false, false, false, amqp.Table{"x-queue-type": style})
	return err
}

func (this amqpChannel) DeclareExchange(name string) error {
	return this.Channel.ExchangeDeclare(name, amqp.ExchangeFanout, true, false, false, false, amqp.Table{})
}
func (this amqpChannel) BindQueue(queue, exchange string) error {
	return this.Channel.QueueBind(queue, "", exchange, false, amqp.Table{})
}

func (this amqpChannel) BufferCapacity(value uint16) error {
	return this.Channel.Qos(int(value), 0, false) // false = per-consumer limit
}
func (this amqpChannel) Consume(consumerID, queue string) (<-chan amqp.Delivery, error) {
	return this.Channel.Consume(queue, consumerID, false, false, false, false, amqp.Table{})
}
func (this amqpChannel) CancelConsumer(consumerID string) error {
	return this.Channel.Cancel(consumerID, true) // noWait: a stuck channel must not hang shutdown; the connection close that follows is bounded
}

func (this amqpChannel) Publish(exchange, key string, envelope amqp.Publishing) error {
	return this.Channel.Publish(exchange, key, false, false, envelope)
}
