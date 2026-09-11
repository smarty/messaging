package sqlmq

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/smarty/gunit"
	"github.com/smarty/gunit/assert/should"
	"github.com/smarty/messaging/v4"
	"github.com/smarty/messaging/v4/sqlmq/adapter"
)

func TestDispatchReceiverFixture(t *testing.T) {
	gunit.Run(new(DispatchReceiverFixture), t)
}

type DispatchReceiverFixture struct {
	*gunit.Fixture

	ctx              context.Context
	ctxShutdown      context.CancelFunc
	channel          chan messaging.Dispatch
	deferredCapacity int
	writer           messaging.CommitWriter
	log              bytes.Buffer

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
	this.initializeDispatchWriter()
}
func (this *DispatchReceiverFixture) initializeDispatchWriter() {
	config := configuration{}
	Options.apply(
		Options.Context(this.ctx),
		Options.StorageHandle(&sql.DB{}),
		Options.Channel(this.channel),
		Options.HandoffTimeout(time.Millisecond*5),
		Options.DeferredHandoffCapacity(this.deferredCapacity),
		Options.Logger(this),
	)(&config)
	config.MessageStore = this
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
	this.So(this.storeWrites, should.Equal, writes)
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

	this.So(err, should.BeNil)
	this.So(len(this.channel), should.Equal, cap(this.channel))
}

func (this *DispatchReceiverFixture) TestWhenHandoffExceedsTimeout_DeferRemainingAndReturnNil() {
	this.channel = make(chan messaging.Dispatch, 1)
	this.channel <- messaging.Dispatch{} // full
	this.initializeDispatchWriter()
	_, _ = this.writer.Write(nil, messaging.Dispatch{MessageID: 1})

	err := this.writer.Commit()

	this.So(err, should.BeNil)
	this.So(this.commitCalls, should.Equal, 1)
	this.So(this.log.String(), should.ContainSubstring,
		"[WARN] Committed [1] message(s) to durable storage, but the dispatch processor did not accept [1] of them within [5ms]. The handoff continues in the background.")
	this.So(receiveDispatch(this.channel), should.Equal, messaging.Dispatch{}) // drain
	this.So(receiveDispatch(this.channel), should.Equal, messaging.Dispatch{MessageID: 1})
}

func (this *DispatchReceiverFixture) TestWhenHandoffPartiallyCompletes_DeferOnlyTheRemainder() {
	this.channel = make(chan messaging.Dispatch, 2)
	this.initializeDispatchWriter()
	_, _ = this.writer.Write(nil, messaging.Dispatch{MessageID: 1}, messaging.Dispatch{MessageID: 2}, messaging.Dispatch{MessageID: 3})

	err := this.writer.Commit()

	this.So(err, should.BeNil)
	this.So(len(this.channel), should.Equal, 2)
	this.So(this.log.String(), should.ContainSubstring,
		"[WARN] Committed [3] message(s) to durable storage, but the dispatch processor did not accept [1] of them within [5ms]. The handoff continues in the background.")
	this.So(receiveDispatch(this.channel), should.Equal, messaging.Dispatch{MessageID: 1})
	this.So(receiveDispatch(this.channel), should.Equal, messaging.Dispatch{MessageID: 2})
	this.So(receiveDispatch(this.channel), should.Equal, messaging.Dispatch{MessageID: 3})
}

func (this *DispatchReceiverFixture) TestWhenDeferredCapacityReached_BlockUntilProcessorAcceptsRemainder() {
	this.deferredCapacity = 1
	this.channel = make(chan messaging.Dispatch, 1)
	this.channel <- messaging.Dispatch{} // full
	this.initializeDispatchWriter()
	_, _ = this.writer.Write(nil, messaging.Dispatch{MessageID: 1}, messaging.Dispatch{MessageID: 2})
	drained := make(chan []messaging.Dispatch, 1)
	go func() {
		time.Sleep(time.Millisecond * 10)
		var results []messaging.Dispatch
		for i := 0; i < 3; i++ {
			results = append(results, receiveDispatch(this.channel))
		}
		drained <- results
	}()

	err := this.writer.Commit()

	this.So(err, should.BeNil)
	this.So(<-drained, should.Equal, []messaging.Dispatch{{}, {MessageID: 1}, {MessageID: 2}})
	this.So(this.log.String(), should.ContainSubstring,
		"[WARN] Deferred handoff capacity [1] reached; waiting for the dispatch processor to accept [2] message(s).")
}

func (this *DispatchReceiverFixture) TestWhenContextEndsDuringHandoff_ReturnNilAndLogRemainingCount() {
	this.channel = make(chan messaging.Dispatch, 1)
	this.channel <- messaging.Dispatch{} // full
	this.initializeDispatchWriter()
	_, _ = this.writer.Write(nil, messaging.Dispatch{MessageID: 1}, messaging.Dispatch{MessageID: 2})
	this.ctxShutdown()

	err := this.writer.Commit()

	this.So(err, should.BeNil)
	this.So(this.commitCalls, should.Equal, 1)
	this.So(len(this.channel), should.Equal, 1)
	this.So(this.log.String(), should.ContainSubstring,
		"[INFO] Context ended during handoff; [2] committed message(s) remain in durable storage for the next startup.")
}

func (this *DispatchReceiverFixture) TestWhenHandoffCompletesInTime_ReturnNilAndLogNothing() {
	_, _ = this.writer.Write(nil, messaging.Dispatch{MessageID: 1}, messaging.Dispatch{MessageID: 2})

	err := this.writer.Commit()

	this.So(err, should.BeNil)
	this.So(len(this.channel), should.Equal, 2)
	this.So(this.log.String(), should.BeBlank)
}

func (this *DispatchReceiverFixture) TestWhenCommittingWithoutAnyDispatches_CommitShouldStillBeInvoked() {
	// there may be other storage operations using the same SQL transaction, so we still need to allow commit to be called
	err := this.writer.Commit()

	this.So(err, should.BeNil)
	this.So(this.commitCalls, should.Equal, 1)
}
func (this *DispatchReceiverFixture) TestWhenCommittingIndicatesDone_ItShouldReturnContextDeadlineExceeded() {
	this.commitError = sql.ErrTxDone
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	this.ctx = ctx
	defer cancel()
	this.initializeDispatchWriter()

	time.Sleep(time.Millisecond * 5)
	err := this.writer.Commit()

	this.So(err, should.Equal, context.DeadlineExceeded)
	this.So(this.commitCalls, should.Equal, 1)
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

func (this *DispatchReceiverFixture) Printf(format string, args ...any) {
	_, _ = fmt.Fprintf(&this.log, format+"\n", args...)
}

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
func (this *DispatchReceiverFixture) Confirm(ctx context.Context, dispatches []messaging.Dispatch) (int, error) {
	panic("nop")
}

func (this *DispatchReceiverFixture) ExecContext(ctx context.Context, statement string, args ...any) (sql.Result, error) {
	panic("nop")
}
func (this *DispatchReceiverFixture) QueryContext(ctx context.Context, statement string, args ...any) (adapter.QueryResult, error) {
	panic("nop")
}
func (this *DispatchReceiverFixture) QueryRowContext(ctx context.Context, statement string, args ...any) adapter.RowScanner {
	panic("nop")
}
func (this *DispatchReceiverFixture) TxHandle() *sql.Tx {
	panic("nop")
}
