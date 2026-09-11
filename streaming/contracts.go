package streaming

import "time"

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
