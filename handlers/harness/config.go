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

	"github.com/smarty/messaging/v3"
)

// New constructs a staged, store-and-forward message-handling pipeline.
// Register domain types (handlers/observers) via Options.Types, and wire
// real Writer, Dispatcher, Serializer, and Monitor collaborators via the
// corresponding Options.* functions. Collaborators default to a shared
// no-op implementation, so omitting them produces a runnable but inert
// pipeline — useful for tests, but not for production.
func New(ctx context.Context, options ...option) (messaging.Handler, []messaging.Listener) {
	var cfg configuration
	for _, apply := range Options.defaults(options...) {
		apply(&cfg)
	}
	return build(ctx, cfg)
}

var Options singleton

type singleton struct{}
type option func(*configuration)

type configuration struct {
	Monitor         Monitor
	Serializer      serializer
	Writer          Writer
	Dispatcher      Dispatcher
	Types           []any
	BatchCapacity   int
	UnitCapacity    int
	UnitSize        int
	SerializerCount int
	ShedThreshold   float64
}

// Types registers the domain objects whose Execute.../Apply... methods drive
// the pipeline. They are passed verbatim to newRouter(...) at build time.
func (singleton) Types(value ...any) option {
	return func(this *configuration) { this.Types = value }
}
func (singleton) Monitor(value Monitor) option {
	return func(this *configuration) { this.Monitor = value }
}
func (singleton) Serializer(value serializer) option {
	return func(this *configuration) { this.Serializer = value }
}
func (singleton) Writer(value Writer) option {
	return func(this *configuration) { this.Writer = value }
}
func (singleton) Dispatcher(value Dispatcher) option {
	return func(this *configuration) { this.Dispatcher = value }
}
func (singleton) BatchCapacity(value int) option {
	return func(this *configuration) { this.BatchCapacity = value }
}
func (singleton) UnitCapacity(value int) option {
	return func(this *configuration) { this.UnitCapacity = value }
}
func (singleton) UnitSize(value int) option {
	return func(this *configuration) { this.UnitSize = value }
}
func (singleton) SerializerCount(value int) option {
	return func(this *configuration) { this.SerializerCount = value }
}
func (singleton) ShedThreshold(value float64) option {
	return func(this *configuration) { this.ShedThreshold = value }
}

func (singleton) defaults(options ...option) []option {
	blank := nop{}
	return append([]option{
		Options.Monitor(blank),
		Options.Serializer(blank),
		Options.Writer(blank),
		Options.Dispatcher(blank),
		Options.BatchCapacity(1024),
		Options.UnitCapacity(1),
		Options.UnitSize(64),
		Options.SerializerCount(4),
		Options.ShedThreshold(0.80),
	}, options...)
}

// nop satisfies every collaborator interface so New(...) can be called with
// zero options and still produce a runnable (if inert) pipeline.
type nop struct{}

func (nop) Track(any)                              {}
func (nop) Serialize(io.Writer, any) error         { return nil }
func (nop) Write(context.Context, ...any) error    { return nil }
func (nop) Dispatch(context.Context, ...any) error { return nil }
