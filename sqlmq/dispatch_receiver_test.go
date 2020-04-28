package sqlmq

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/messaging/v3"
	"github.com/smartystreets/messaging/v3/sqlmq/adapter"
)

func TestDispatchReceiverFixture(t *testing.T) {
	gunit.Run(new(DispatchReceiverFixture), t)
}

type DispatchReceiverFixture struct {
	*gunit.Fixture

	ctx         context.Context
	ctxShutdown context.CancelFunc
	channel     chan messaging.Dispatch
	writer      messaging.CommitWriter

	commitCalls   int
	commitError   error
	rollbackCalls int
	rollbackError error

	storeContext context.Context
	storeWrites  []messaging.Dispatch
	storeError   error
}

func (this *DispatchReceiverFixture) Setup() {
	this.ctx, this.ctxShutdown = context.WithCancel(context.Background())
	this.channel = make(chan messaging.Dispatch, 16)
	config := configuration{Channel: this.channel, MessageStore: this}
	this.writer = newDispatchReceiver(this.ctx, this, config)
}

func (this *DispatchReceiverFixture) TestWhenWritingDispatches_ReturnNumberOfWritesNewlyBuffered() {
	written, err := this.writer.Write(nil, []messaging.Dispatch{{}, {}, {}, {}, {}}...)

	this.So(written, should.Equal, 5)
	this.So(err, should.BeNil)
}

func (this *DispatchReceiverFixture) TestWhenCommitting_FlushBufferToStorageThenCommitAndSendBufferToOutputChannel() {
	writes := []messaging.Dispatch{
		{MessageType: "1", Payload: []byte("a")},
		{MessageType: "2", Payload: []byte("b")},
		{MessageType: "3", Payload: []byte("c")},
	}
	_, _ = this.writer.Write(nil, writes...)

	err := this.writer.Commit()

	this.So(err, should.BeNil)
	this.So(this.storeContext, should.Equal, this.ctx)
	this.So(this.storeWrites, should.Resemble, writes)
	this.So(this.commitCalls, should.Equal, 1)
	this.So(len(this.channel), should.Equal, len(writes))
}
func (this *DispatchReceiverFixture) TestWhenUnderlyingStoreOperationsFails_ReturnErrorDoNotCommitOrSendToOutputChannel() {
	this.storeError = errors.New("")
	_, _ = this.writer.Write(nil, messaging.Dispatch{MessageType: "1", Payload: []byte("a")})

	err := this.writer.Commit()

	this.So(err, should.Equal, this.storeError)
	this.So(this.commitCalls, should.Equal, 0)
	this.So(this.channel, should.BeEmpty)
}
func (this *DispatchReceiverFixture) TestWhenUnderlyingCommitFails_ReturnErrorAndDoNotSendToOutputChannel() {
	this.commitError = errors.New("")
	_, _ = this.writer.Write(nil, messaging.Dispatch{MessageType: "1", Payload: []byte("a")})

	err := this.writer.Commit()

	this.So(err, should.Equal, this.commitError)
	this.So(this.commitCalls, should.Equal, 1)
	this.So(this.channel, should.BeEmpty)
}
func (this *DispatchReceiverFixture) TestWhenNoDispatchesWritten_CommitShouldStillInvokeUnderlyingCommit() {
	err := this.writer.Commit()

	this.So(err, should.BeNil)
	this.So(this.storeWrites, should.BeEmpty)
	this.So(this.commitCalls, should.Equal, 1)
	this.So(this.channel, should.BeEmpty)
}
func (this *DispatchReceiverFixture) TestWhenTransactionContextCancelled_DoNotBlockWhenWritingToOutputChannel() {
	for i := 0; i < cap(this.channel); i++ {
		this.channel <- messaging.Dispatch{}
	}
	_, _ = this.writer.Write(nil, messaging.Dispatch{MessageType: "1", Payload: []byte("a")})
	this.ctxShutdown()

	err := this.writer.Commit()

	this.So(err, should.Equal, context.Canceled)
	this.So(len(this.channel), should.Equal, cap(this.channel))
}

func (this *DispatchReceiverFixture) TestWhenRollingBack_InvokeUnderlyingTransactionRollback() {
	this.rollbackError = errors.New("")

	err := this.writer.Rollback()

	this.So(err, should.Equal, this.rollbackError)
}
func (this *DispatchReceiverFixture) TestWhenClosing_Nop() {
	err := this.writer.Close()

	this.So(err, should.BeNil)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *DispatchReceiverFixture) Commit() error   { this.commitCalls++; return this.commitError }
func (this *DispatchReceiverFixture) Rollback() error { return this.rollbackError }

func (this *DispatchReceiverFixture) Store(ctx context.Context, writer adapter.Writer, writes []messaging.Dispatch) error {
	this.So(writer, should.Equal, this)

	this.storeContext = ctx
	this.storeWrites = append(this.storeWrites, writes...)
	return this.storeError
}
func (this *DispatchReceiverFixture) Load(ctx context.Context, id uint64) ([]messaging.Dispatch, error) {
	panic("nop")
}
func (this *DispatchReceiverFixture) Confirm(ctx context.Context, dispatches []messaging.Dispatch) error {
	panic("nop")
}

func (this *DispatchReceiverFixture) ExecContext(ctx context.Context, statement string, args ...interface{}) (sql.Result, error) {
	panic("nop")
}
func (this *DispatchReceiverFixture) QueryContext(ctx context.Context, statement string, args ...interface{}) (adapter.QueryResult, error) {
	panic("nop")
}
func (this *DispatchReceiverFixture) QueryRowContext(ctx context.Context, statement string, args ...interface{}) adapter.RowScanner {
	panic("nop")
}
func (this *DispatchReceiverFixture) TxHandle() *sql.Tx {
	panic("nop")
}
