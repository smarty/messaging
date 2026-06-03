package harness

import (
	"bytes"
)

type execution struct {
	monitor     Monitor
	maxUnitSize int
	input       chan *batch
	output      chan *unitOfWork
	executor    executor
}

func newExecution(monitor Monitor, maxUnitSize int, input chan *batch, output chan *unitOfWork, exec executor) *execution {
	return &execution{
		monitor:     monitor,
		maxUnitSize: maxUnitSize,
		input:       input,
		output:      output,
		executor:    exec,
	}
}

func (this *execution) Listen() {
	defer close(this.output)

	var unit *unitOfWork // TODO: pool for *unitOfWork (and this.monitor.Track(UnitOfWorkComplete{}) when putting back)
	for item := range this.input {
		if unit == nil {
			unit = new(unitOfWork)
		}
		unit.completions = append(unit.completions, item.complete)
		for _, message := range item.messages {
			this.executor.Execute(message, func(outgoing ...any) {
				for _, result := range outgoing {
					record := &Message{Value: result, Content: bytes.NewBuffer(nil)} // TODO: pool for *Message
					unit.results = append(unit.results, record)
				}
			})
		}
		if len(unit.completions) < this.maxUnitSize && len(this.input) > 0 {
			continue // more to do
		}
		this.output <- unit
		unit = nil
	}
}
