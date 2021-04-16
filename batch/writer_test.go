package batch

import (
	"context"
	"errors"
	"testing"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/messaging/v4"
)

func TestWriterFixture(t *testing.T) {
	gunit.Run(new(WriterFixture), t)
}

type WriterFixture struct {
	*gunit.Fixture

	ctx      context.Context
	shutdown context.CancelFunc
	writer   messaging.Writer

	connectCount   int
	connectContext context.Context
	connectError   error

	commitWriterCount   int
	commitWriterContext context.Context
	commitWriterError   error

	writeContext    context.Context
	writeDispatches []messaging.Dispatch
	writeError      error

	commitCount   int
	commitError   error
	rollbackCount int

	closeCount int
}

func (this *WriterFixture) Setup() {
	this.ctx, this.shutdown = context.WithCancel(context.Background())
	this.initializeWriter()
}
func (this *WriterFixture) initializeWriter() {
	this.writer = NewWriter(this)
}

func (this *WriterFixture) TestWhenEmptySetOfDispatches_Nop() {
	this.shutdown()

	count, err := this.writer.Write(this.ctx)

	this.So(count, should.BeZeroValue)
	this.So(err, should.BeNil)
}
func (this *WriterFixture) TestWhenWritingWithAClosedContext_ReturnContextError() {
	this.shutdown()

	count, err := this.writer.Write(this.ctx, messaging.Dispatch{})

	this.So(count, should.BeZeroValue)
	this.So(err, should.Equal, context.Canceled)
}

func (this *WriterFixture) TestWhenWritingTheFirstTime_ItShouldOpenANewConnectionAndCommitWriter() {
	dispatches := []messaging.Dispatch{{MessageID: 1}, {MessageID: 2}}
	count, err := this.writer.Write(this.ctx, dispatches...)

	this.So(count, should.Equal, len(dispatches))
	this.So(err, should.BeNil)

	this.So(this.connectContext, should.Equal, this.ctx)
	this.So(this.commitWriterContext, should.Equal, this.ctx)
	this.So(this.writeContext, should.Equal, this.ctx)
	this.So(this.writeDispatches, should.Resemble, dispatches)
	this.So(this.commitCount, should.Equal, 1)
	this.So(this.closeCount, should.Equal, 0)
}

func (this *WriterFixture) TestWhenOpeningConnectionFails_ItShouldReturnUnderlyingError() {
	this.connectError = errors.New("")

	count, err := this.writer.Write(this.ctx, messaging.Dispatch{})

	this.So(count, should.Equal, 0)
	this.So(err, should.Equal, this.connectError)
	this.So(this.closeCount, should.Equal, 0)
}
func (this *WriterFixture) TestWhenOpeningCommitWriterFails_ItShouldCloseConnectionAndReturnUnderlyingError() {
	this.commitWriterError = errors.New("")

	count, err := this.writer.Write(this.ctx, messaging.Dispatch{})

	this.So(count, should.Equal, 0)
	this.So(err, should.Equal, this.commitWriterError)
	this.So(this.closeCount, should.Equal, 1)
}
func (this *WriterFixture) TestWhenWritingDispatchesFails_ItShouldCloseResourcesAndReturnUnderlyingError() {
	this.writeError = errors.New("")

	count, err := this.writer.Write(this.ctx, messaging.Dispatch{})

	this.So(count, should.Equal, 0)
	this.So(err, should.Equal, this.writeError)
	this.So(this.closeCount, should.Equal, 2)
}
func (this *WriterFixture) TestWhenCommittingWrittenDispatchesFails_ItShouldCloseResourcesAndReturnUnderlyingError() {
	this.commitError = errors.New("")

	count, err := this.writer.Write(this.ctx, messaging.Dispatch{})

	this.So(count, should.Equal, 0)
	this.So(err, should.Equal, this.commitError)
	this.So(this.closeCount, should.Equal, 2)
}

func (this *WriterFixture) TestWhenWritingMultipleTimes_ItShouldUseExistingConnectionAndWriter() {
	_, _ = this.writer.Write(this.ctx, messaging.Dispatch{}, messaging.Dispatch{}, messaging.Dispatch{})

	_, _ = this.writer.Write(this.ctx, messaging.Dispatch{}, messaging.Dispatch{})

	this.So(this.connectCount, should.Equal, 1)
	this.So(this.commitWriterCount, should.Equal, 1)
	this.So(this.writeDispatches, should.HaveLength, 5)
	this.So(this.commitCount, should.Equal, 2)
}

func (this *WriterFixture) TestWhenClosing_ItShouldCloseUnderlyingConnectionAndWriter() {
	_, _ = this.writer.Write(this.ctx, messaging.Dispatch{})

	err := this.writer.Close()

	this.So(err, should.BeNil)
	this.So(this.closeCount, should.Equal, 2)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *WriterFixture) Connect(ctx context.Context) (messaging.Connection, error) {
	this.connectCount++
	this.connectContext = ctx
	if this.connectError != nil {
		return nil, this.connectError
	} else {
		return this, nil
	}
}

func (this *WriterFixture) Reader(ctx context.Context) (messaging.Reader, error) {
	panic("nop")
}
func (this *WriterFixture) Writer(ctx context.Context) (messaging.Writer, error) {
	panic("nop")
}
func (this *WriterFixture) CommitWriter(ctx context.Context) (messaging.CommitWriter, error) {
	this.commitWriterCount++
	this.commitWriterContext = ctx

	if this.commitWriterError != nil {
		return nil, this.commitWriterError
	} else {
		return this, nil
	}
}

func (this *WriterFixture) Write(ctx context.Context, dispatches ...messaging.Dispatch) (int, error) {
	this.writeContext = ctx
	this.writeDispatches = append(this.writeDispatches, dispatches...)
	return len(dispatches), this.writeError
}
func (this *WriterFixture) Commit() error {
	this.commitCount++
	return this.commitError
}
func (this *WriterFixture) Rollback() error {
	this.rollbackCount++
	return nil
}

func (this *WriterFixture) Close() error {
	this.closeCount++
	return nil
}
