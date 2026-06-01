package harness

import (
	"context"
	"errors"
	"io"
)

// Exported collaborator interfaces — callers supply real implementations via Options.*.

type (
	Writer interface {
		Write(ctx context.Context, messages ...any) error
	}
	Dispatcher interface {
		Dispatch(ctx context.Context, messages ...any) error
	}
	Monitor interface {
		Track(observation any)
	}
)

// Unexported internal interfaces — discovered reflectively from domain types
// supplied via Options.Types(...).

type (
	executor interface {
		Execute(message any, broadcast func(...any))
	}
	applicator interface {
		Apply(message any)
	}
	serializer interface {
		Serialize(out io.Writer, in any) error
	}
)

// Monitor observations emitted by the pipeline.

type (
	BatchInFlight      struct{}
	BatchComplete      struct{}
	LoadShed           struct{}
	CallerDeparted     struct{}
	UnitOfWorkInFlight struct{}
	UnitOfWorkComplete struct{}
	SerializationError struct {
		Value any
		Error error
	}
	PersistenceError struct {
		Attempt int
		Error   error
	}
	PersistenceAbandoned struct{ Attempts int }
	BroadcastError       struct {
		Attempt int
		Error   error
	}
	BroadcastAbandoned struct{ Attempts int }
)

var (
	ErrSerialization = errors.New("serialization error")
	ErrPersistence   = errors.New("persistence error")
	ErrBroadcast     = errors.New("broadcast error")
)

// Unexported value types shared across pipeline stages.

type (
	batch struct {
		ctx      context.Context
		messages []any
		complete func()
	}
	unitOfWork struct {
		results     []*Message
		completions []func()
	}
)
