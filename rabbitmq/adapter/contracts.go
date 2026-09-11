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
	// (not on first call), so a registration window never drops a
	// notification. The channel closes when the connection closes.
	BlockedNotifications() <-chan amqp.Blocking
	// CloseNotifications reports a close the broker or the network initiated.
	// The channel closes when the connection shuts down for any reason.
	CloseNotifications() <-chan *amqp.Error

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

	// CloseNotifications reports a channel close the broker initiated (a 404,
	// a 406 acknowledgement timeout). CancelNotifications reports a consumer
	// the broker cancelled (its queue was deleted). Both close when the
	// channel shuts down. A fake may return nil for either.
	CloseNotifications() <-chan *amqp.Error
	CancelNotifications() <-chan string

	io.Closer
}
