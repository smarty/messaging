package harness

import (
	"context"
	"io"
	"sync"
	"testing"

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
	this.subject = newEntrypoint(this, this.work)
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
	this.So(item.ctx.Value("testing"), should.Equal, this.Name())
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
		this.So(item.ctx.Value("testing"), should.Equal, this.Name())
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
