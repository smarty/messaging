package messaging

import (
	"context"
	"io"
	"time"
)

type Handler interface {
	Handle(ctx context.Context, messages ...interface{})
}

type Connector interface {
	Connect(ctx context.Context) (Connection, error)
	io.Closer
}
type Connection interface {
	Reader(ctx context.Context) (Reader, error)
	Writer(ctx context.Context) (Writer, error)
	CommitWriter(ctx context.Context) (CommitWriter, error)
	io.Closer
}

type Reader interface {
	Stream(ctx context.Context, config StreamConfig) (Stream, error)
	io.Closer
}
type StreamConfig struct {
	// Re-establishes the topology with the underlying messaging infrastructure, if necessary.
	// On RabbitMQ, this will re-create queues and exchanges and then bind the associated queue to those exchanges.
	EstablishTopology bool

	// Indicates whether the stream is the only one that will be opened with the broker.
	ExclusiveStream bool

	// The number of messages that will be buffered in local memory from the messaging infrastructure.
	BufferCapacity uint16

	// The name of the stream to be read. For RabbitMQ, this is the name of the queue, with Kafka, it's the name of the
	// topic.
	StreamName string

	// If supported by the underlying messaging infrastructure, the topics to which the stream should subscribe. In the
	// case of RabbitMQ, the names of the exchanges to which the stream should subscribe. In cases like Kafka, this
	// value is ignored and the StreamName above becomes the topic.
	Topics []string

	// If supported by the underlying messaging infrastructure, the partition which should be read from. In cases like
	// RabbitMQ, this value is ignored. With Kafka, this value is used to subscribe to the appropriate partition.
	Partition uint64

	// If supported by the underlying messaging infrastructure, the sequence at which messages should be read from
	// the topic. In RabbitMQ, this value is ignored. With Kafka, this value is the starting index on the topic.
	Sequence uint64
}
type Stream interface {
	Read(ctx context.Context, delivery *Delivery) error
	Acknowledge(ctx context.Context, deliveries ...Delivery) error
	io.Closer
}

type Writer interface {
	Write(ctx context.Context, dispatches ...Dispatch) (int, error)
	io.Closer
}
type CommitWriter interface {
	Writer
	Commit() error
	Rollback() error
}

type Dispatch struct {
	SourceID        uint64
	MessageID       uint64
	CorrelationID   uint64 // FUTURE: CausationID and UserID
	Timestamp       time.Time
	Expiration      time.Duration
	Durable         bool
	Topic           string
	Partition       uint64
	MessageType     string
	ContentType     string
	ContentEncoding string
	Payload         []byte
	Headers         map[string]interface{}
	Message         interface{}
}
type Delivery struct {
	Upstream        interface{} // the raw, upstream message provided by the messaging infrastructure
	DeliveryID      uint64
	SourceID        uint64
	MessageID       uint64
	CorrelationID   uint64 // FUTURE: CausationID and UserID
	Timestamp       time.Time
	Durable         bool
	MessageType     string
	ContentType     string
	ContentEncoding string
	Payload         []byte
	Headers         map[string]interface{}
	Message         interface{}
}

type Listener interface {
	Listen()
}
type ListenCloser interface {
	Listener
	io.Closer
}
