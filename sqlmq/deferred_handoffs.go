package sqlmq

import (
	"context"
	"sync/atomic"

	"github.com/smarty/messaging/v4"
)

// deferredHandoffs finishes outbox handoffs that outlived their HandoffTimeout.
// It is shared by every receiver built from one configuration and caps the
// number of committed messages held in memory across all background handoffs.
// Its goroutines run on a lifetime context derived from the configured
// process context, not on the caller's, so a request-scoped caller whose
// request has ended does not strand its committed rows until the next
// restart. The dispatch processor closes the tracker when it stops.
type deferredHandoffs struct {
	ctx      context.Context
	cancel   context.CancelFunc
	output   chan messaging.Dispatch
	capacity int
	pending  atomic.Int64
}

func newDeferredHandoffs(lifetime context.Context, output chan messaging.Dispatch, capacity int) *deferredHandoffs {
	ctx, cancel := context.WithCancel(lifetime)
	return &deferredHandoffs{ctx: ctx, cancel: cancel, output: output, capacity: capacity}
}

// Context is the lifetime every background handoff runs on. Receivers use it
// to tell "the process is stopping" apart from "my caller's request ended".
func (this *deferredHandoffs) Context() context.Context { return this.ctx }

// TryDefer takes ownership of the dispatches and keeps sending them into the
// output channel from a background goroutine until they are all accepted or
// the lifetime ends. It reports false, and takes nothing, when holding these
// dispatches would exceed the capacity.
func (this *deferredHandoffs) TryDefer(dispatches []messaging.Dispatch) bool {
	count := int64(len(dispatches))
	if this.pending.Add(count) > int64(this.capacity) {
		this.pending.Add(-count)
		return false
	}
	go func() {
		defer this.pending.Add(-count)
		for _, dispatch := range dispatches {
			select {
			case this.output <- dispatch:
			case <-this.ctx.Done():
				return // the rows are durable; the next startup publishes them
			}
		}
	}()
	return true
}

// Close ends every background handoff. Rows they still held stay durable.
func (this *deferredHandoffs) Close() error {
	this.cancel()
	return nil
}
