package harness

import (
	"context"
	"fmt"
	"time"
)

type broadcast struct {
	ctx        context.Context
	monitor    Monitor
	input      chan *unitOfWork
	output     chan *unitOfWork
	buffer     []*Message
	dispatcher Dispatcher
	wait       func(context.Context, time.Duration) error
}

func newBroadcast(ctx context.Context, monitor Monitor, input, output chan *unitOfWork, dispatcher Dispatcher, wait func(context.Context, time.Duration) error) *broadcast {
	return &broadcast{
		ctx:        ctx,
		monitor:    monitor,
		input:      input,
		output:     output,
		buffer:     make([]*Message, 0, 1024),
		dispatcher: dispatcher,
		wait:       wait,
	}
}

func (this *broadcast) Listen() {
	defer close(this.output)
	for unit := range this.input {
		for _, message := range unit.results {
			this.buffer = append(this.buffer, message)
		}
		this.dispatch()
		this.buffer = this.buffer[:0]
		// Unlike persistence (which drops the unit on abandonment so MQ redelivers),
		// broadcast always forwards: the batch is already durably stored, so we ack
		// upstream regardless of whether dispatch succeeded.
		this.output <- unit
	}
}
func (this *broadcast) dispatch() {
	var failure BroadcastError
	for attempt := 1; ; attempt++ {
		err := this.dispatcher.Dispatch(this.ctx, this.buffer...)
		if err == nil {
			return
		}
		failure.Attempt = attempt
		failure.Error = fmt.Errorf("%w: %w", ErrBroadcast, err)
		this.monitor.Track(failure)
		// Retries forever (until the process restarts) unless the context is cancelled.
		// TODO: exponential backoff w/ jitter
		if this.wait(this.ctx, time.Second) != nil {
			this.monitor.Track(BroadcastAbandoned{Attempts: attempt})
			return
		}
	}
}
