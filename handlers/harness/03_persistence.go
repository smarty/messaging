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
	sleep   func(time.Duration)
	buffer  []any
}

func newPersistence(ctx context.Context, monitor Monitor, input, output chan *unitOfWork, writer Writer, sleep func(time.Duration)) *persistence {
	return &persistence{
		ctx:     ctx,
		monitor: monitor,
		input:   input,
		output:  output,
		writer:  writer,
		sleep:   sleep,
		buffer:  make([]any, 0, 1024),
	}
}

func (this *persistence) Listen() {
	var failure PersistenceError

	defer close(this.output)
	for unit := range this.input {
		for _, message := range unit.results {
			this.buffer = append(this.buffer, message)
		}
		for attempt := 1; ; attempt++ {
			err := this.writer.Write(this.ctx, this.buffer...)
			if err == nil {
				failure.Attempt = 0
				failure.Error = nil
				break
			}
			failure.Attempt = attempt
			failure.Error = fmt.Errorf("%w: %w", ErrPersistence, err)
			this.monitor.Track(failure)
			this.sleep(time.Second) // TODO: exponential back-off w/ jitter
		}
		this.output <- unit
		this.buffer = this.buffer[:0]
	}
}
