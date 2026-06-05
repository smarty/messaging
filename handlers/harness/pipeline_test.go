package harness

import (
	"context"
	"io"
	"sync"
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/better"
	"github.com/smarty/gunit/v2/assert/should"
)

func TestPipelineFixture(t *testing.T) {
	gunit.Run(new(PipelineFixture), t)
}

type PipelineFixture struct {
	*gunit.Fixture
	ctx      context.Context
	pipeline Pipeline
	waiter   sync.WaitGroup

	executeLock    sync.Mutex
	executeCalls   []any
	executeOutputs [][]any

	writeLock     sync.Mutex
	writeCalls    [][]*Message
	dispatchLock  sync.Mutex
	dispatchCalls [][]*Message

	trackLock sync.Mutex
	tracked   []any
}

type commandType string

func (this *PipelineFixture) Setup() {
	this.ctx = context.WithValue(this.Context(), "testing", this.Name())
	this.pipeline = New(this.ctx,
		Options.Types(this),
		Options.Monitor(this),
		Options.Serializer(this),
		Options.Writer(this),
		Options.Dispatcher(this),
	)
	for _, listener := range this.pipeline.Listeners {
		this.waiter.Go(listener.Listen)
	}
}

// ExecuteCommand is picked up by scan() as the Execute-prefixed method driving
// the pipeline: every Handle(ctx, msg) call dispatches into this method.
func (this *PipelineFixture) ExecuteCommand(_ commandType, broadcast func(...any)) {
	this.executeLock.Lock()
	defer this.executeLock.Unlock()
	if len(this.executeOutputs) == 0 {
		return
	}
	out := this.executeOutputs[0]
	this.executeOutputs = this.executeOutputs[1:]
	broadcast(out...)
}

// Execute wraps the fixture so it also satisfies the package-private executor
// interface directly — which is what scan() will pick up. We count calls here.
func (this *PipelineFixture) Execute(message any, broadcast func(...any)) {
	this.executeLock.Lock()
	this.executeCalls = append(this.executeCalls, message)
	this.executeLock.Unlock()
	this.ExecuteCommand(commandType(""), broadcast)
}

func (this *PipelineFixture) Serialize(out io.Writer, _ any) error {
	_, _ = out.Write([]byte("encoded"))
	return nil
}

func (this *PipelineFixture) ContentType() string { return "" }

func (this *PipelineFixture) Write(ctx context.Context, messages ...*Message) error {
	this.So(ctx.Value("testing"), should.Equal, this.Name())
	buffer := make([]*Message, len(messages))
	copy(buffer, messages)
	this.writeLock.Lock()
	this.writeCalls = append(this.writeCalls, buffer)
	this.writeLock.Unlock()
	return nil
}

func (this *PipelineFixture) Dispatch(ctx context.Context, messages ...*Message) error {
	this.So(ctx.Value("testing"), should.Equal, this.Name())
	captured := make([]*Message, len(messages))
	copy(captured, messages)
	this.dispatchLock.Lock()
	this.dispatchCalls = append(this.dispatchCalls, captured)
	this.dispatchLock.Unlock()
	return nil
}

func (this *PipelineFixture) Track(observation any) {
	this.trackLock.Lock()
	defer this.trackLock.Unlock()
	this.tracked = append(this.tracked, observation)
}

func (this *PipelineFixture) countTracked() (batchInFlight, batchComplete int) {
	this.trackLock.Lock()
	defer this.trackLock.Unlock()
	for _, observation := range this.tracked {
		switch observation.(type) {
		case BatchInFlight:
			batchInFlight++
		case BatchComplete:
			batchComplete++
		}
	}
	return batchInFlight, batchComplete
}

func (this *PipelineFixture) shutdown() {
	closer, ok := this.pipeline.BlockingEntrypoint.(io.Closer)
	this.So(ok, better.BeTrue)
	this.So(closer.Close(), should.BeNil)
	this.waiter.Wait()
}

func (this *PipelineFixture) TestPipelineRoutesMessageThroughExecutionPersistenceBroadcast() {
	this.executeOutputs = [][]any{{"event-A", "event-B"}}

	this.pipeline.BlockingEntrypoint.Handle(this.ctx, commandType("command-1"))
	this.shutdown()

	this.So(len(this.executeCalls), should.Equal, 1)

	this.So(len(this.writeCalls), should.Equal, 1)
	this.So(len(this.writeCalls[0]), should.Equal, 2)
	this.So(this.writeCalls[0][0].Value, should.Equal, "event-A")
	this.So(this.writeCalls[0][1].Value, should.Equal, "event-B")

	this.So(len(this.dispatchCalls), should.Equal, 1)
	this.So(len(this.dispatchCalls[0]), should.Equal, 2)
	this.So(this.dispatchCalls[0][0].Value, should.Equal, "event-A")
	this.So(this.dispatchCalls[0][1].Value, should.Equal, "event-B")

	batchInFlight, batchComplete := this.countTracked()
	this.So(batchInFlight, should.Equal, 1)
	this.So(batchComplete, should.Equal, 1)
}

func (this *PipelineFixture) TestPipelineHandlesMultipleMessagesAcrossHandleCalls() {
	this.executeOutputs = [][]any{{"e1"}, {"e2"}, {"e3"}}

	this.pipeline.BlockingEntrypoint.Handle(this.ctx, commandType("c1"))
	this.pipeline.BlockingEntrypoint.Handle(this.ctx, commandType("c2"))
	this.pipeline.BlockingEntrypoint.Handle(this.ctx, commandType("c3"))
	this.shutdown()

	this.So(len(this.executeCalls), should.Equal, 3)

	var written []any
	for _, call := range this.writeCalls {
		for _, message := range call {
			written = append(written, message.Value)
		}
	}
	this.So(written, should.Equal, []any{"e1", "e2", "e3"})

	var dispatched []any
	for _, call := range this.dispatchCalls {
		for _, message := range call {
			dispatched = append(dispatched, message.Value)
		}
	}
	this.So(dispatched, should.Equal, []any{"e1", "e2", "e3"})

	batchInFlight, batchComplete := this.countTracked()
	this.So(batchInFlight, should.Equal, 3)
	this.So(batchComplete, should.Equal, 3)
}

func (this *PipelineFixture) TestPipelineShutsDownWithNoTraffic() {
	this.shutdown()
	this.So(len(this.executeCalls), should.Equal, 0)
	this.So(len(this.writeCalls), should.Equal, 0)
	this.So(len(this.dispatchCalls), should.Equal, 0)
	this.So(this.tracked, should.BeEmpty)
}

func (this *PipelineFixture) TestPipelineSerializesEachBroadcastResult() {
	this.executeOutputs = [][]any{{map[string]int{"value": 42}}}

	this.pipeline.BlockingEntrypoint.Handle(this.ctx, commandType("go"))
	this.shutdown()

	this.So(len(this.writeCalls), should.Equal, 1)
	message := this.writeCalls[0][0]
	this.So(message.Content.Len() > 0, should.BeTrue)
}
