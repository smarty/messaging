package adapter

import (
	"context"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type amqpConnector struct{}

func (this amqpConnector) Connect(_ context.Context, socket net.Conn, config Config) (Connection, error) {
	plainAuth := &amqp.PlainAuth{Username: config.Username, Password: config.Password}
	amqpConfig := amqp.Config{
		SASL:      []amqp.Authentication{plainAuth},
		Vhost:     config.VirtualHost,
		Heartbeat: config.Heartbeat,
	}

	if connection, err := amqp.Open(socket, amqpConfig); err != nil {
		return nil, err
	} else {
		return amqpConnection{Connection: connection}, nil
	}
}

type amqpConnection struct{ *amqp.Connection }

// Close bounds the close handshake with a deadline on the underlying socket.
// Setting the deadline also unblocks any write already wedged on a broker
// that has stopped reading (a resource alarm), so Close cannot hang.
func (this amqpConnection) Close() error {
	return this.Connection.CloseDeadline(time.Now().Add(closeGracePeriod))
}

const closeGracePeriod = time.Second * 5

func (this amqpConnection) Channel() (Channel, error) {
	if channel, err := this.Connection.Channel(); err != nil {
		return nil, err
	} else {
		return amqpChannel{Channel: channel}, nil
	}
}

type amqpChannel struct{ *amqp.Channel }

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
	return this.Channel.Cancel(consumerID, false)
}

func (this amqpChannel) Publish(exchange, key string, envelope amqp.Publishing) error {
	return this.Channel.Publish(exchange, key, false, false, envelope)
}
