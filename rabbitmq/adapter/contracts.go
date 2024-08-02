package adapter

import (
	"context"
	"io"
	"net"

	amqp "github.com/rabbitmq/amqp091-go"
)

func New() Connector { return amqpConnector{} }

type Config struct {
	Username    string
	Password    string
	VirtualHost string
}

type Connector interface {
	Connect(ctx context.Context, socket net.Conn, config Config) (Connection, error)
}

type Connection interface {
	Channel() (Channel, error)
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
