// Package harness provides a staged, store-and-forward message-handling
// pipeline composed of goroutine stages (entrypoint, execution, serialization,
// persistence, completion, broadcast, terminal) connected by buffered channels.
//
// Callers register domain objects whose Execute.../Apply... methods drive the
// pipeline via Options.Types(...), and supply collaborators (Writer, Dispatcher,
// Serializer, Monitor) via the corresponding Options.*. All collaborators
// default to a no-op implementation, so omitting them produces a runnable but
// inert pipeline — useful for tests, but not for production.
//
// The only exported entry point is New(ctx, options...); every internal stage
// type is unexported and cannot be constructed directly by callers.
//
// The persistence and broadcast stages retry their collaborators on failure;
// those retry loops abort when the context passed to New(ctx, ...) is cancelled,
// so consumers must cancel it on shutdown to avoid hanging the drain. Custom
// Writer and Dispatcher implementations must honor the context they are given.
package harness

import (
	"context"
	"io"
	"net/http"

	"github.com/smarty/messaging/v3"
)

// New constructs a staged, store-and-forward message-handling pipeline.
// Register domain types (handlers/observers) via Options.Types, and wire
// real Writer, Dispatcher, Serializer, and Monitor collaborators via the
// corresponding Options.* functions. Collaborators default to a shared
// no-op implementation, so omitting them produces a runnable but inert
// pipeline — useful for tests, but not for production.
func New(ctx context.Context, options ...option) Pipeline {
	var cfg configuration
	for _, apply := range Options.defaults(options...) {
		apply(&cfg)
	}
	return build(ctx, cfg)
}

type Pipeline struct {
	// SheddingHTTPWrapper is meant to wrap around any http.Handler that calls SheddingEntrypoint.
	// It responds with HTTP 503 in the event that the handler is backed up beyond the configured ShedThreshold.
	SheddingHTTPWrapper func(http.Handler) http.Handler

	// SheddingEntrypoint is a messaging.Handler that is meant to be guarded by an admitter (such as SheddingHTTPWrapper).
	SheddingEntrypoint messaging.Handler

	// BlockingEntrypoint is a messaging.Handler that will block until the results of the provided work have been durably stored.
	BlockingEntrypoint messaging.Handler

	// Listeners contains each phase of the harness pipeline (serialization, persistence, broadcast, etc.).
	// Each listener should be invoked on a separate goroutine by a component like github.com/smarty/dominoes.
	Listeners []messaging.Listener
}

var Options singleton

type singleton struct{}
type option func(*configuration)

type configuration struct {
	monitor                Monitor
	serializer             serializer
	writer                 Writer
	dispatcher             Dispatcher
	types                  []any
	burstCapacity          int
	pipelineBufferCapacity int
	executionUnitSize      int
	serializerCount        int
	shedThreshold          float64
}

// Types registers the domain objects whose Execute.../Apply... methods drive
// the pipeline. They are passed verbatim to newRouter(...) at build time.
func (singleton) Types(value ...any) option {
	return func(this *configuration) { this.types = value }
}

// Monitor sets the Monitor collaborator that receives pipeline observations
// (BatchInFlight, BatchComplete, LoadShed, SerializationError, etc.).
func (singleton) Monitor(value Monitor) option {
	return func(this *configuration) { this.monitor = value }
}

// Serializer sets the collaborator used to encode outgoing messages into bytes.
func (singleton) Serializer(value serializer) option {
	return func(this *configuration) { this.serializer = value }
}

// Writer sets the collaborator that persists encoded messages (e.g. to a database or message store).
func (singleton) Writer(value Writer) option {
	return func(this *configuration) { this.writer = value }
}

// Dispatcher sets the collaborator that broadcasts outgoing messages to downstream consumers.
func (singleton) Dispatcher(value Dispatcher) option {
	return func(this *configuration) { this.dispatcher = value }
}

// BurstCapacity sets the buffer size of the channel between the entrypoint and
// execution stages. Larger values absorb more burst traffic before back-pressure
// reaches callers. Default: 1024.
func (singleton) BurstCapacity(value int) option {
	return func(this *configuration) { this.burstCapacity = value }
}

// PipelineBufferCapacity sets the buffer size of the channels connecting all pipeline
// stages after execution (serialization → persistence → completion → broadcast →
// terminal). Default: 4.
func (singleton) PipelineBufferCapacity(value int) option {
	return func(this *configuration) { this.pipelineBufferCapacity = value }
}

// ExecutionUnitSize sets the maximum number of batches coalesced into a single unit of
// work before the execution stage flushes downstream. Higher values increase
// throughput at the cost of latency per batch. Default: 64.
func (singleton) ExecutionUnitSize(value int) option {
	return func(this *configuration) { this.executionUnitSize = value }
}

// SerializerCount sets the number of concurrent serialization goroutines.
// Default: 4.
func (singleton) SerializerCount(value int) option {
	return func(this *configuration) { this.serializerCount = value }
}

// ShedThreshold sets the load-shedding threshold as a fraction of BurstCapacity
// in the range [0, 1]. When the batch channel fill ratio meets or exceeds this
// value, new callers are refused (admission returns 503; Handle is a no-op).
// This option only affects HTTP callers.
// Default: 0.80.
func (singleton) ShedThreshold(value float64) option {
	return func(this *configuration) { this.shedThreshold = value }
}

func (singleton) defaults(options ...option) []option {
	blank := nop{}
	return append([]option{
		Options.Monitor(blank),
		Options.Serializer(blank),
		Options.Writer(blank),
		Options.Dispatcher(blank),
		Options.BurstCapacity(1024),
		Options.PipelineBufferCapacity(4),
		Options.ExecutionUnitSize(64),
		Options.SerializerCount(4),
		Options.ShedThreshold(0.80),
	}, options...)
}

// nop satisfies every collaborator interface so New(...) can be called with
// zero options and still produce a runnable (if inert) pipeline.
type nop struct{}

func (nop) Track(any)                              {}
func (nop) Serialize(io.Writer, any) error         { return nil }
func (nop) ContentType() string                    { return "" }
func (nop) Write(context.Context, ...any) error    { return nil }
func (nop) Dispatch(context.Context, ...any) error { return nil }
