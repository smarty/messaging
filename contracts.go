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
	// If not specified, the provider-specific default value is used.
	BufferCapacity uint16

	// The maximum number of bytes allowed per message. Messages containing more bytes than this will be truncated.
	// If not specified, the provider-specific default value is used.
	MaxMessageBytes uint32

	// The name of the stream to be read. For RabbitMQ, this is the name of the queue. With Kafka, this is the name of a
	// topic for an individual consumer that is not part of a consumer group.
	StreamName string

	// For Kafka, the name of the consumer group. When this value is specified, other values such as Topics must now be
	// specified while other values such as Partition and Sequence are ignored.
	GroupName string

	// If supported by the underlying messaging infrastructure, the topics to which the stream should subscribe. In the
	// case of RabbitMQ, the names of the exchanges to which the stream should subscribe. In the case of Kafka, this
	// value is used when a consumer group GroupName is specified, otherwise, it is ignored.
	Topics []string

	// If supported by the underlying messaging infrastructure, the partition which should be read from. In cases like
	// RabbitMQ, this value is ignored. With Kafka, this value is used to subscribe to the appropriate partition for an
	// individual consumer that is not part of a consumer group.
	Partition uint64

	// If supported by the underlying messaging infrastructure, the sequence at which messages should be read from
	// the topic. In RabbitMQ, this value is ignored. With Kafka, this value is the starting index on the topic of an
	// individual consumer that is not part of a consumer group.
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
