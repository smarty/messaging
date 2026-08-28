package adapter

import (
	"context"
	"io"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func New() Connector { return amqpConnector{} }

type Config struct {
	Username    string
	Password    string
	VirtualHost string
	Heartbeat   time.Duration
}

type Connector interface {
	Connect(ctx context.Context, socket net.Conn, config Config) (Connection, error)
}

type Connection interface {
	Channel() (Channel, error)

	// BlockedNotifications returns a channel of broker blocked/unblocked
	// notifications. The implementation registers the channel during Connect
	// (not on first call), so no notification is dropped in a registration
	// window; the channel closes when the connection closes.
	BlockedNotifications() <-chan amqp.Blocking

	io.Closer
}

type Channel interface {
	DeclareQueue(name string, replicated bool) error
	DeclareExchange(name string) error
	BindQueue(queue, exchange string) error

	BufferCapacity(value uint16) error
	Consume(consumerID, queue string) (<-chan amqp.Delivery, error)
	Ack(deliveryTag uint64, multiple bool) error
	CancelConsumer(consumerID string) error

	Publish(exchange, key string, envelope amqp.Publishing) error
	Tx() error
	TxCommit() error
	TxRollback() error

	io.Closer
}
