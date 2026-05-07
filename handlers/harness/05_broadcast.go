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
	buffer     []any
	dispatcher Dispatcher
	sleep      func(time.Duration)
}

func newBroadcast(ctx context.Context, monitor Monitor, input, output chan *unitOfWork, dispatcher Dispatcher, sleep func(time.Duration)) *broadcast {
	return &broadcast{
		ctx:        ctx,
		monitor:    monitor,
		input:      input,
		output:     output,
		buffer:     make([]any, 0, 1024),
		dispatcher: dispatcher,
		sleep:      sleep,
	}
}

func (this *broadcast) Listen() {
	var failure BroadcastError

	defer close(this.output)
	for unit := range this.input {
		for _, message := range unit.results {
			this.buffer = append(this.buffer, message)
		}
		for attempt := 1; ; attempt++ {
			err := this.dispatcher.Dispatch(this.ctx, this.buffer...)
			if err == nil {
				failure.Attempt = 0
				failure.Error = nil
				break
			}
			failure.Attempt = attempt
			failure.Error = fmt.Errorf("%w: %w", ErrBroadcast, err)
			this.monitor.Track(failure)
			this.sleep(time.Second) // TODO: exponential backoff w/ jitter
		}
		this.buffer = this.buffer[:0]
		this.output <- unit
	}
}
