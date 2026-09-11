package sqlmq

import (
	"context"
	"sync/atomic"

	"github.com/smarty/messaging/v4"
)

// deferredHandoffs finishes outbox handoffs that outlived their HandoffTimeout.
// It is shared by every receiver built from one configuration and caps the
// number of committed messages held in memory across all background handoffs.
type deferredHandoffs struct {
	output   chan messaging.Dispatch
	capacity int
	pending  atomic.Int64
}

func newDeferredHandoffs(output chan messaging.Dispatch, capacity int) *deferredHandoffs {
	return &deferredHandoffs{output: output, capacity: capacity}
}

// TryDefer takes ownership of the dispatches and keeps sending them into the
// output channel from a background goroutine until they are all accepted or
// the context ends. It reports false, and takes nothing, when holding these
// dispatches would exceed the capacity.
func (this *deferredHandoffs) TryDefer(ctx context.Context, dispatches []messaging.Dispatch) bool {
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
			case <-ctx.Done():
				return // the rows are durable; the next startup publishes them
			}
		}
	}()
	return true
}
