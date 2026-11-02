package streaming

import "time"

// closedReporter is implemented by connections that can say whether they are
// already dead (rabbitmq's connection does). The pool uses it to replace a
// cached connection instead of handing it to the next subscriber.
type closedReporter interface {
	Closed() bool
}

type logger interface {
	Printf(format string, args ...any)
}

// monitor receives one callback per notable consumer event. Callbacks run on
// the subscriber or worker goroutine, and BatchHandled and BatchAcknowledged
// fire once per batch on the hot path, so implementations must be cheap.
// streamName is the queue name given to NewSubscription; use it as a label
// so one implementation can serve every subscription in a process.
type monitor interface {
	// StreamOpened fires once per attempt to open a subscription's stream.
	// A nil error means the stream is live; otherwise err names the step
	// that failed (connection, reader, or stream) and the runtime retries
	// after ReconnectDelay.
	StreamOpened(streamName string, err error)
	// StreamClosed fires once when a live stream's session ends, for any
	// reason. A reconnect follows unless the consumer is shutting down.
	StreamClosed(streamName string)
	// BatchHandled fires after Handler.Handle returns or panics, with the
	// number of messages handed to the handler and the wall time it took,
	// including any retries an inner retry handler performed.
	BatchHandled(streamName string, count int, duration time.Duration)
	// BatchAcknowledged fires after each acknowledgement attempt with the
	// number of deliveries in the batch. A non-nil error ends the worker
	// and the broker redelivers the batch after reconnect.
	BatchAcknowledged(streamName string, count int, err error)
	// ShutdownForced fires when workers miss the subscription's shutdown
	// timeout and the hard context is cancelled; in-flight deliveries are
	// abandoned unacknowledged.
	ShutdownForced(streamName string)
}

// DeliveryAgeMonitor is an optional extension of the monitor given to
// Options.Monitor. When that monitor also implements it, each worker reports how
// long its deliveries waited between being stamped (when sqlmq stored them, or
// when the transport published them) and reaching the handler. It is exported so
// implementers can assert at compile time that their method matches; a mismatch
// would otherwise be silent.
type DeliveryAgeMonitor interface {
	// DeliveriesAged fires once per batch, just before Handler.Handle, with the
	// age of each delivery handed to the handler. Deliveries without a
	// timestamp are skipped, and a batch with none fires nothing. Ages compare
	// the producer's clock with the consumer's, so skew between hosts can make
	// one negative. The AMQP transport carries timestamps in whole seconds, so
	// an age can read up to a second high. The slice is reused for the next
	// batch: do not retain it.
	DeliveriesAged(streamName string, ages ...time.Duration)
}
