package harness

import (
	"context"
	"fmt"
	"time"
)

type persistence struct {
	ctx     context.Context
	monitor Monitor
	input   chan *unitOfWork
	output  chan *unitOfWork
	writer  Writer
	wait    func(context.Context, time.Duration) error
	buffer  []*Message
}

func newPersistence(ctx context.Context, monitor Monitor, input, output chan *unitOfWork, writer Writer, wait func(context.Context, time.Duration) error) *persistence {
	return &persistence{
		ctx:     ctx,
		monitor: monitor,
		input:   input,
		output:  output,
		writer:  writer,
		wait:    wait,
		buffer:  make([]*Message, 0, 1024),
	}
}

func (this *persistence) Listen() {
	defer close(this.output)
	for unit := range this.input {
		for _, message := range unit.results {
			this.buffer = append(this.buffer, message)
		}
		stored := this.store()
		this.buffer = this.buffer[:0]
		if !stored {
			continue // shutdown before durable write: do NOT forward (no ack); MQ redelivers
		}
		this.output <- unit
	}
}
func (this *persistence) store() (stored bool) {
	var failure PersistenceError
	for attempt := 1; ; attempt++ {
		err := this.writer.Write(this.ctx, this.buffer...)
		if err == nil {
			return true
		}
		failure.Attempt = attempt
		failure.Error = fmt.Errorf("%w: %w", ErrPersistence, err)
		this.monitor.Track(failure)
		// Retries forever (until the process restarts) unless the context is cancelled.
		// TODO: exponential back-off w/ jitter
		if this.wait(this.ctx, time.Second) != nil {
			this.monitor.Track(PersistenceAbandoned{Attempts: attempt})
			return false
		}
	}
}
