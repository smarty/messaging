package harness

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
)

func TestEntrypointFixture(t *testing.T) {
	gunit.Run(new(EntrypointFixture), t)
}

type EntrypointFixture struct {
	*gunit.Fixture
	ctx     context.Context
	work    chan *batch
	subject *entrypoint

	trackMu sync.Mutex
	tracked []any
}

func (this *EntrypointFixture) Setup() {
	this.ctx = context.WithValue(this.Context(), "testing", this.Name())
	this.work = make(chan *batch, 4)
	this.subject = newEntrypoint(this, this.work, 0.80)
}

func (this *EntrypointFixture) Track(observation any) {
	this.trackMu.Lock()
	defer this.trackMu.Unlock()
	this.tracked = append(this.tracked, observation)
}

func (this *EntrypointFixture) TestImplementsCloser() {
	var _ io.Closer = this.subject
}

func (this *EntrypointFixture) TestHandlePushesBatchAndBlocksUntilCompletion() {
	done := make(chan struct{})
	go func() {
		this.subject.Handle(this.ctx, "msg-1", "msg-2")
		close(done)
	}()

	item := <-this.work
	this.So(item.messages, should.Equal, []any{"msg-1", "msg-2"})

	select {
	case <-done:
		this.Fatal("Handle returned before complete() was invoked")
	default:
	}

	item.complete()
	<-done

	this.So(this.tracked, should.HaveLength, 2)
	this.So(this.tracked, should.Contain, BatchInFlight{})
	this.So(this.tracked, should.Contain, BatchComplete{})
}

func (this *EntrypointFixture) TestHandleSerializesMultipleConcurrentCalls() {
	done := make(chan struct{}, 3)
	go func() { this.subject.Handle(this.ctx, "a"); done <- struct{}{} }()
	go func() { this.subject.Handle(this.ctx, "b"); done <- struct{}{} }()
	go func() { this.subject.Handle(this.ctx, "c"); done <- struct{}{} }()

	for range 3 {
		item := <-this.work
		item.complete()
		<-done
	}

	this.So(this.tracked, should.HaveLength, 6)
	var inFlight int
	for _, observation := range this.tracked {
		switch observation.(type) {
		case BatchInFlight:
			inFlight++
		case BatchComplete:
			inFlight--
		default:
			this.Fatal("Unexpected observation:", observation)
		}
	}
	this.So(inFlight, should.Equal, 0)
}

func (this *EntrypointFixture) TestAwait_ReturnsAfterCompletion() {
	done := make(chan struct{})
	go func() {
		this.subject.await(this.ctx, "msg")
		close(done)
	}()

	item := <-this.work
	this.So(item.messages, should.Equal, []any{"msg"})

	select {
	case <-done:
		this.Fatal("await returned before complete() was invoked")
	default:
	}

	item.complete()
	<-done

	this.So(this.tracked, should.Contain, BatchInFlight{})
	this.So(this.tracked, should.Contain, BatchComplete{})
}

func (this *EntrypointFixture) TestAwait_UnblocksOnContextCancelWhileWaiting() {
	ctx, cancel := context.WithCancel(this.ctx)
	defer cancel()

	done := make(chan struct{})
	go func() {
		this.subject.await(ctx, "msg")
		close(done)
	}()

	item := <-this.work // enqueue succeeded; pipeline now owns the batch.

	select {
	case <-done:
		this.Fatal("await returned before the caller departed or completion")
	default:
	}

	cancel()
	<-done

	this.So(this.tracked, should.Contain, BatchInFlight{})
	this.So(this.tracked, should.Contain, CallerDeparted{})

	// The batch was NOT abandoned: the pipeline still owns it and will invoke
	// complete() later. await must not have Put it back to the pool.
	item.complete()
	this.So(this.tracked, should.Contain, BatchComplete{})
}

func (this *EntrypointFixture) TestAwait_UnblocksOnContextCancelWhileEnqueuing() {
	work := make(chan *batch, 1)
	subject := newEntrypoint(this, work, 0.80)
	work <- &batch{} // fill the channel so the next enqueue blocks.

	ctx, cancel := context.WithCancel(this.ctx)
	defer cancel()

	done := make(chan struct{})
	go func() {
		subject.await(ctx, "msg")
		close(done)
	}()

	select {
	case <-done:
		this.Fatal("await returned before the enqueue could block")
	default:
	}

	cancel()
	<-done

	this.So(this.tracked, should.Contain, CallerDeparted{})
	this.So(this.tracked, should.NOT.Contain, BatchInFlight{})

	// The never-enqueued batch was abandoned and returned to the pool.
	this.So(this.tracked, should.NOT.Contain, BatchComplete{})
}

func (this *EntrypointFixture) TestAwait_DepartedInFlightDoesNotCorruptPooledWaiter() {
	const workers = 8
	const perWorker = 2000

	work := make(chan *batch, 64)
	subject := newEntrypoint(this, work, 0.80)

	// Drainer: take ownership of each enqueued batch and complete it promptly,
	// driving the waiter's count to zero concurrently with new awaits that recycle
	// waiters from the pool. If a departed await recycles a still-in-use waiter, a
	// later prepare()'s Add(1) races the detached Wait()'s return -- tripping the
	// runtime's WaitGroup misuse detector.
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for item := range work {
			item.complete()
		}
	}()

	var clients sync.WaitGroup
	for range workers {
		clients.Add(1)
		go func() {
			defer clients.Done()
			for range perWorker {
				ctx, cancel := context.WithCancel(this.ctx)
				go cancel() // depart at an arbitrary moment relative to processing.
				subject.await(ctx, "msg")
			}
		}()
	}
	clients.Wait()

	this.So(subject.Close(), should.BeNil)
	<-drained

	this.So(this.tracked, should.Contain, CallerDeparted{})
	this.So(this.tracked, should.Contain, BatchComplete{})
}

func (this *EntrypointFixture) TestAwait_BatchCarriesExactlyOneMessage() {
	go this.subject.await(this.ctx, "only")

	item := <-this.work
	this.So(item.messages, should.HaveLength, 1)
	this.So(item.messages[0], should.Equal, "only")

	item.complete()
}

func (this *EntrypointFixture) TestAwait_ClosedPipelineReturnsImmediately() {
	this.So(this.subject.Close(), should.BeNil)

	done := make(chan struct{})
	go func() {
		this.subject.await(this.ctx, "msg")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		this.Fatal("await did not return on a closed pipeline")
	}

	this.So(this.tracked, should.BeEmpty)
}

func (this *EntrypointFixture) TestAdmit_TrueWhenBelowThreshold() {
	this.So(this.subject.admit(), should.BeTrue)
}

func (this *EntrypointFixture) TestAdmit_FalseAtOrAboveThreshold_TracksLoadShed() {
	work := make(chan *batch, 10)
	subject := newEntrypoint(this, work, 0.5)
	for range 5 {
		work <- &batch{}
	}
	this.So(subject.admit(), should.BeFalse)
	this.So(this.tracked, should.Contain, LoadShed{})
}

func (this *EntrypointFixture) TestAdmit_FalseWhenClosed_NoLoadShed() {
	this.So(this.subject.Close(), should.BeNil)
	this.So(this.subject.admit(), should.BeFalse)
	this.So(this.tracked, should.NOT.Contain, LoadShed{})
}

func (this *EntrypointFixture) TestAdmit_ThresholdAtOrAboveOneDisablesWatermark() {
	work := make(chan *batch, 4)
	subject := newEntrypoint(this, work, 2.0)
	for range 4 {
		work <- &batch{}
	}
	this.So(subject.admit(), should.BeTrue)
	this.So(this.tracked, should.NOT.Contain, LoadShed{})

	this.So(subject.Close(), should.BeNil)
	this.So(subject.admit(), should.BeFalse)
}

func (this *EntrypointFixture) TestHandle_BlocksUntilDurable() {
	done := make(chan struct{})
	go func() {
		this.subject.Handle(this.ctx, "msg")
		close(done)
	}()

	item := <-this.work

	select {
	case <-done:
		this.Fatal("Handle returned before the batch was completed")
	case <-time.After(20 * time.Millisecond):
	}

	item.complete()
	<-done
}

func (this *EntrypointFixture) TestHandle_DoesNotShedAtHighWatermark() {
	work := make(chan *batch, 2)
	subject := newEntrypoint(this, work, 0.5)

	done := make(chan struct{}, 5)
	for range 5 {
		go func() {
			subject.Handle(this.ctx, "msg")
			done <- struct{}{}
		}()
	}

	// None should return while the work is unconsumed: Handle blocks on send
	// (no shedding) and then waits for completion.
	select {
	case <-done:
		this.Fatal("Handle returned before the batch was completed")
	case <-time.After(20 * time.Millisecond):
	}

	for range 5 {
		item := <-work
		item.complete()
	}
	for range 5 {
		<-done
	}

	this.So(this.tracked, should.NOT.Contain, LoadShed{})
}

func (this *EntrypointFixture) TestHandle_IgnoresContextCancel() {
	ctx, cancel := context.WithCancel(this.ctx)
	defer cancel()

	done := make(chan struct{})
	go func() {
		this.subject.Handle(ctx, "msg")
		close(done)
	}()

	item := <-this.work
	cancel()

	select {
	case <-done:
		this.Fatal("Handle returned on context cancel; MQ deliveries must not honor a deadline")
	case <-time.After(20 * time.Millisecond):
	}

	item.complete()
	<-done
}

func (this *EntrypointFixture) TestHandle_ReturnsImmediatelyOnClosedPipeline() {
	this.So(this.subject.Close(), should.BeNil)

	done := make(chan struct{})
	go func() {
		this.subject.Handle(this.ctx, "msg")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		this.Fatal("Handle did not return on a closed pipeline")
	}
}

func (this *EntrypointFixture) TestHandle_PreservesVariadicMessages() {
	go this.subject.Handle(this.ctx, "a", "b", "c")

	item := <-this.work
	this.So(item.messages, should.HaveLength, 3)
	this.So(item.messages, should.Equal, []any{"a", "b", "c"})

	item.complete()
}

func (this *EntrypointFixture) TestCloseReleasesListenAndClosesWorkChannel() {
	listened := make(chan struct{})
	go func() {
		this.subject.Listen()
		close(listened)
	}()

	this.So(this.subject.Close(), should.BeNil)

	<-listened

	_, open := <-this.work
	this.So(open, should.BeFalse)
	this.So(this.tracked, should.BeEmpty)
}

func (this *EntrypointFixture) TestCloseIsIdempotent() {
	this.So(this.subject.Close(), should.BeNil)
	this.So(this.subject.Close(), should.BeNil)
	this.So(this.tracked, should.BeEmpty)
}
