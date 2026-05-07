package harness

import (
	"fmt"
)

type serialization struct {
	monitor    Monitor
	serializer serializer
	input      chan *unitOfWork
	output     chan *unitOfWork
}

func newSerialization(monitor Monitor, enc serializer, input, output chan *unitOfWork) *serialization {
	return &serialization{
		monitor:    monitor,
		serializer: enc,
		input:      input,
		output:     output,
	}
}

func (this *serialization) Listen() {
	var failure SerializationError

	defer close(this.output)
	for unit := range this.input {
		for _, message := range unit.results {
			err := this.serializer.Serialize(message.Content, message.Value)
			if err != nil {
				failure.Error = fmt.Errorf("%w: %w", ErrSerialization, err)
				failure.Value = message.Value
				this.monitor.Track(failure)
			}
			failure.Error = nil
			failure.Value = nil
		}
		this.output <- unit
	}
}
