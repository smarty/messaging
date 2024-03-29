package messaging

import (
	"context"
	"errors"
	"io"
	"time"
)

// 'External' contracts:

type (
	Listener interface {
		Listen()
	}
	ListenCloser interface {
		Listener
		io.Closer
	}
	Handler interface {
		Handle(ctx context.Context, messages ...any)
	}
)

// Connecting:

type (
	Connector interface {
		Connect(ctx context.Context) (Connection, error)
		io.Closer
	}
	Connection interface {
		Reader(ctx context.Context) (Reader, error)
		Writer(ctx context.Context) (Writer, error)
		CommitWriter(ctx context.Context) (CommitWriter, error)
		io.Closer
	}
)

// Reading:

type (
	Delivery struct {
		Upstream        any // the raw, upstream message provided by the messaging infrastructure
		DeliveryID      uint64
		SourceID        uint64
		MessageID       uint64
		CorrelationID   uint64 // FUTURE: CausationID and UserID
		Timestamp       time.Time
		Durable         bool
		Topic           string
		Partition       uint64
		MessageType     string
		ContentType     string
		ContentEncoding string
		Payload         []byte
		Headers         map[string]any
		Message         any
	}
	Reader interface {
		Stream(ctx context.Context, config StreamConfig) (Stream, error)
		io.Closer
	}
	StreamConfig struct {
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
		// topic for an individual consumer when not acting as part of a consumer group.
		StreamName string

		// For RabbitMQ, indicates whether to use replicated or "quorum queues" which are raft-based queues with higher
		// durability guarantees because messages are replicated to other nodes. This setting will slightly increase the
		// per-dispatch and per-delivery latencies when writing and/or reading messages.
		StreamReplication bool

		// For Kafka, the name of the consumer group. When this value is specified, other values such as Topics must now be
		// specified while other values such as Partition and Sequence are ignored.
		GroupName string

		// If supported by the underlying messaging infrastructure, the topics to which the stream should subscribe. In the
		// case of RabbitMQ, the names of the exchanges to which the stream should subscribe. In the case of Kafka, this
		// value is used when a consumer group GroupName is specified, otherwise, it is ignored.
		Topics []string

		// If supported by the underlying messaging infrastructure, the list of available topics that should exist with
		// the message broker. In the case of RabbitMQ, the names of the exchanges which should be declared.
		AvailableTopics []string

		// If supported by the underlying messaging infrastructure, the partition which should be read from. In cases like
		// RabbitMQ, this value is ignored. With Kafka, this value is used to subscribe to the appropriate partition for an
		// individual consumer when not acting as part of a consumer group.
		Partition uint64

		// If supported by the underlying messaging infrastructure, the sequence at which messages should be read from
		// the topic. In RabbitMQ, this value is ignored. With Kafka, this value is the starting index on the topic of an
		// individual consumer that is not part of a consumer group.
		Sequence uint64
	}
	Stream interface {
		Read(ctx context.Context, delivery *Delivery) error
		Acknowledge(ctx context.Context, deliveries ...Delivery) error
		io.Closer
	}
)

// Writing:

type (
	Dispatch struct {
		SourceID        uint64
		MessageID       uint64
		CorrelationID   uint64 // FUTURE: CausationID and UserID
		Timestamp       time.Time
		Expiration      time.Duration
		Durable         bool
		Topic           string
		Partition       uint64 // Not the partition to send to, but instead the PartitionKey to be used (by a hashing algorithm) to decide which partition to send the message to.
		MessageType     string
		ContentType     string
		ContentEncoding string
		Payload         []byte
		Headers         map[string]any
		Message         any
	}
	Writer interface {
		Write(ctx context.Context, dispatches ...Dispatch) (int, error)
		io.Closer
	}
	CommitWriter interface {
		Writer
		Commit() error
		Rollback() error
	}
)

var ErrEmptyDispatchTopic = errors.New("the destination topic is missing")
