package harness

import (
	"sync"

	"github.com/smarty/messaging/v3"
)

type stationFactory func(in, out chan *unitOfWork) messaging.Listener

func newFanOut(factory stationFactory, workerCount, unitCapacity int, input, finalOutput chan *unitOfWork) []messaging.Listener {
	var (
		listeners = make([]messaging.Listener, workerCount)
		outputs   = make([]chan *unitOfWork, workerCount)
	)
	for i := range workerCount {
		outputs[i] = make(chan *unitOfWork, unitCapacity)
		listeners[i] = factory(input, outputs[i])
	}
	return append(listeners, newFanIn(outputs, finalOutput))
}

type fanIn struct {
	inputs []chan *unitOfWork
	output chan *unitOfWork
}

func newFanIn(inputs []chan *unitOfWork, output chan *unitOfWork) *fanIn {
	return &fanIn{
		inputs: inputs,
		output: output,
	}
}

func (this *fanIn) Listen() {
	defer close(this.output)
	var wg sync.WaitGroup
	defer wg.Wait()
	for _, input := range this.inputs {
		wg.Go(func() {
			for unit := range input {
				this.output <- unit
			}
		})
	}
}
