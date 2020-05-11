package sqlmq

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/messaging/v3"
)

func TestDispatchSenderFixture(t *testing.T) {
	gunit.Run(new(DispatchSenderFixture), t)
}

type DispatchSenderFixture struct {
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

func (this *DispatchSenderFixture) Setup() {
	this.ctx, this.shutdown = context.WithCancel(context.Background())
	this.initializeDispatchSender()
}
func (this *DispatchSenderFixture) initializeDispatchSender() {
	config := configuration{}
	Options.apply(
		Options.Context(this.ctx),
		Options.StorageHandle(&sql.DB{}),
		Options.TransportConnector(this),
	)(&config)
	this.writer = newDispatchSender(config)
}

func (this *DispatchSenderFixture) TestWhenEmptySetOfDispatches_Nop() {
	this.shutdown()

	count, err := this.writer.Write(this.ctx)

	this.So(count, should.BeZeroValue)
	this.So(err, should.BeNil)
}
func (this *DispatchSenderFixture) TestWhenWritingWithAClosedContext_ReturnContextError() {
	this.shutdown()

	count, err := this.writer.Write(this.ctx, messaging.Dispatch{})

	this.So(count, should.BeZeroValue)
	this.So(err, should.Equal, context.Canceled)
}

func (this *DispatchSenderFixture) TestWhenWritingTheFirstTime_ItShouldOpenANewConnectionAndCommitWriter() {
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

func (this *DispatchSenderFixture) TestWhenOpeningConnectionFails_ItShouldReturnUnderlyingError() {
	this.connectError = errors.New("")

	count, err := this.writer.Write(this.ctx, messaging.Dispatch{})

	this.So(count, should.Equal, 0)
	this.So(err, should.Equal, this.connectError)
	this.So(this.closeCount, should.Equal, 0)
}
func (this *DispatchSenderFixture) TestWhenOpeningCommitWriterFails_ItShouldCloseConnectionAndReturnUnderlyingError() {
	this.commitWriterError = errors.New("")

	count, err := this.writer.Write(this.ctx, messaging.Dispatch{})

	this.So(count, should.Equal, 0)
	this.So(err, should.Equal, this.commitWriterError)
	this.So(this.closeCount, should.Equal, 1)
}
func (this *DispatchSenderFixture) TestWhenWritingDispatchesFails_ItShouldCloseResourcesAndReturnUnderlyingError() {
	this.writeError = errors.New("")

	count, err := this.writer.Write(this.ctx, messaging.Dispatch{})

	this.So(count, should.Equal, 0)
	this.So(err, should.Equal, this.writeError)
	this.So(this.closeCount, should.Equal, 2)
}
func (this *DispatchSenderFixture) TestWhenCommittingWrittenDispatchesFails_ItShouldCloseResourcesAndReturnUnderlyingError() {
	this.commitError = errors.New("")

	count, err := this.writer.Write(this.ctx, messaging.Dispatch{})

	this.So(count, should.Equal, 0)
	this.So(err, should.Equal, this.commitError)
	this.So(this.closeCount, should.Equal, 2)
}

func (this *DispatchSenderFixture) TestWhenWritingMultipleTimes_ItShouldUseExistingConnectionAndWriter() {
	_, _ = this.writer.Write(this.ctx, messaging.Dispatch{}, messaging.Dispatch{}, messaging.Dispatch{})

	_, _ = this.writer.Write(this.ctx, messaging.Dispatch{}, messaging.Dispatch{})

	this.So(this.connectCount, should.Equal, 1)
	this.So(this.commitWriterCount, should.Equal, 1)
	this.So(this.writeDispatches, should.HaveLength, 5)
	this.So(this.commitCount, should.Equal, 2)
}

func (this *DispatchSenderFixture) TestWhenClosing_ItShouldCloseUnderlyingConnectionAndWriter() {
	_, _ = this.writer.Write(this.ctx, messaging.Dispatch{})

	err := this.writer.Close()

	this.So(err, should.BeNil)
	this.So(this.closeCount, should.Equal, 2)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *DispatchSenderFixture) Connect(ctx context.Context) (messaging.Connection, error) {
	this.connectCount++
	this.connectContext = ctx
	if this.connectError != nil {
		return nil, this.connectError
	} else {
		return this, nil
	}
}

func (this *DispatchSenderFixture) Reader(ctx context.Context) (messaging.Reader, error) {
	panic("nop")
}
func (this *DispatchSenderFixture) Writer(ctx context.Context) (messaging.Writer, error) {
	panic("nop")
}
func (this *DispatchSenderFixture) CommitWriter(ctx context.Context) (messaging.CommitWriter, error) {
	this.commitWriterCount++
	this.commitWriterContext = ctx

	if this.commitWriterError != nil {
		return nil, this.commitWriterError
	} else {
		return this, nil
	}
}

func (this *DispatchSenderFixture) Write(ctx context.Context, dispatches ...messaging.Dispatch) (int, error) {
	this.writeContext = ctx
	this.writeDispatches = append(this.writeDispatches, dispatches...)
	return len(dispatches), this.writeError
}
func (this *DispatchSenderFixture) Commit() error {
	this.commitCount++
	return this.commitError
}
func (this *DispatchSenderFixture) Rollback() error {
	this.rollbackCount++
	return nil
}

func (this *DispatchSenderFixture) Close() error {
	this.closeCount++
	return nil
}