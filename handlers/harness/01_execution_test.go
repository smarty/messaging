package harness

import (
	"sync"
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
)

func TestExecutionFixture(t *testing.T) {
	gunit.Run(new(ExecutionFixture), t)
}

type ExecutionFixture struct {
	*gunit.Fixture
	input   chan *batch
	output  chan *unitOfWork
	subject *execution

	executeMu      sync.Mutex
	executeCalls   []any
	executeOutputs [][]any

	tracked []any
}

func (this *ExecutionFixture) Setup() {
	this.input = make(chan *batch, 8)
	this.output = make(chan *unitOfWork, 8)
	this.subject = newExecution(this, 64, this.input, this.output, this)
}

func (this *ExecutionFixture) Execute(message any, broadcast func(...any)) {
	this.executeMu.Lock()
	defer this.executeMu.Unlock()
	this.executeCalls = append(this.executeCalls, message)
	if len(this.executeOutputs) == 0 {
		return
	}
	out := this.executeOutputs[0]
	this.executeOutputs = this.executeOutputs[1:]
	broadcast(out...)
}

func (this *ExecutionFixture) Track(observation any) {
	this.tracked = append(this.tracked, observation)
}

func (this *ExecutionFixture) drain() (results []*unitOfWork) {
	for unit := range this.output {
		results = append(results, unit)
	}
	return results
}

func (this *ExecutionFixture) TestSingleBatchProducesUnitOfWork() {
	this.executeOutputs = [][]any{{"result-A"}}
	this.input <- &batch{messages: []any{"msg-A"}, complete: func() {}}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 1)
	this.So(this.executeCalls, should.Equal, []any{"msg-A"})
	this.So(len(units[0].results), should.Equal, 1)
	this.So(units[0].results[0].Value, should.Equal, "result-A")
	this.So(len(units[0].completions), should.Equal, 1)
}

func (this *ExecutionFixture) TestUnitFlushesWhenMaxUnitSizeReached() {
	this.executeOutputs = [][]any{{"r1"}, {"r2"}, {"r3"}}
	this.input <- &batch{messages: []any{"m1"}, complete: func() {}}
	this.input <- &batch{messages: []any{"m2"}, complete: func() {}}
	this.input <- &batch{messages: []any{"m3"}, complete: func() {}}
	close(this.input)

	this.subject = newExecution(this, 2, this.input, this.output, this)
	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 2)
	this.So(len(units[0].completions), should.Equal, 2)
	this.So(len(units[1].completions), should.Equal, 1)
	this.So(this.executeCalls, should.Equal, []any{"m1", "m2", "m3"})
}

func (this *ExecutionFixture) TestEmptyExecutorOutputProducesUnitWithNoResults() {
	this.input <- &batch{messages: []any{"silent"}, complete: func() {}}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 1)
	this.So(units[0].results, should.BeEmpty)
	this.So(this.executeCalls, should.Equal, []any{"silent"})
}

func (this *ExecutionFixture) TestExecutorBroadcastsMultipleResults() {
	this.executeOutputs = [][]any{{"r1", "r2", "r3"}}
	this.input <- &batch{messages: []any{"msg"}, complete: func() {}}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 1)
	this.So(len(units[0].results), should.Equal, 3)
	this.So(units[0].results[0].Value, should.Equal, "r1")
	this.So(units[0].results[1].Value, should.Equal, "r2")
	this.So(units[0].results[2].Value, should.Equal, "r3")
}

func (this *ExecutionFixture) TestClosedInputClosesOutput() {
	close(this.input)
	go this.subject.Listen()

	_, open := <-this.output
	this.So(open, should.BeFalse)
	this.So(this.tracked, should.BeEmpty)
}
