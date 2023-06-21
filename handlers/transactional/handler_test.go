package transactional

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/smarty/assertions/should"
	"github.com/smarty/gunit"
	"github.com/smarty/messaging/v3"
)

func TestFixture(t *testing.T) {
	gunit.Run(new(Fixture), t)
}

type Fixture struct {
	*gunit.Fixture

	ctx      context.Context
	messages []any
	handler  messaging.Handler
	sqlTx    *sql.Tx

	connectCount   int
	connectContext context.Context
	connectError   error

	commitWriterCount   int
	commitWriterContext context.Context
	commitWriterError   error

	commitCount   int
	commitError   error
	rollbackCount int
	rollbackError error

	closeCount int

	factoryState State
	factoryCount int
	factoryError error

	handleContext  context.Context
	handleMessages []any
	handleError    error

	monitor              *FakeMonitor
	monitorCommitCount   int
	monitorRollbackCount int
	monitorBeginErrors   []error
	monitorCommitErrors  []error
}

func (this *Fixture) Setup() {
	this.ctx = context.Background()
	this.messages = []any{1, 2, 3}
	this.sqlTx = &sql.Tx{}
	this.monitor = &FakeMonitor{fixture: this}
	this.handler = New(this, this.factory, Options.Monitor(this.monitor), Options.Logger(nop{}))
}
func (this *Fixture) handle() {
	this.handler.Handle(this.ctx, this.messages...)
}

func (this *Fixture) TestNilPanic_ItShouldPanic() {
	this.ctx = nil
	this.So(this.handle, should.PanicWith, errNilContext)
}
func (this *Fixture) TestWhenOpeningConnectionFails_ItShouldPanic() {
	this.connectError = errors.New("")

	this.So(this.handle, should.PanicWith, this.connectError)
	this.So(this.connectContext, should.Equal, this.ctx)
	this.So(this.monitorBeginErrors, should.Resemble, []error{this.connectError})
}
func (this *Fixture) TestWhenOpeningCommitWriterFails_ItShouldPanic() {
	this.commitWriterError = errors.New("")

	this.So(this.handle, should.PanicWith, this.commitWriterError)
	this.So(this.monitorBeginErrors, should.Resemble, []error{this.commitWriterError})
	this.So(this.commitWriterContext, should.Resemble, &transactionalContext{
		Context:    this.ctx,
		Connection: this,
		Tx:         this.sqlTx,
	})
	this.So(this.closeCount, should.Equal, 1) // close connection
}

func (this *Fixture) TestWhenInvokingFactoryPanics_ItShouldPanicAndRollBack() {
	this.factoryError = errors.New("")

	this.So(this.handle, should.PanicWith, this.factoryError)
	this.So(this.factoryState, should.Resemble, State{Tx: this.sqlTx, Writer: this})
	this.So(this.closeCount, should.Equal, 2) // close connection and writer
	this.So(this.rollbackCount, should.Equal, 1)
}
func (this *Fixture) TestWhenInvokingHandlerPanics_ItShouldPanicAndRollback() {
	this.handleError = errors.New("")

	this.So(this.handle, should.PanicWith, this.handleError)
	this.So(this.closeCount, should.Equal, 2) // close connection and writer
	this.So(this.rollbackCount, should.Equal, 1)
	this.So(this.handleContext, should.Equal, this.ctx)
	this.So(this.handleMessages, should.Resemble, this.messages)
}

func (this *Fixture) TestWhenHandlerCompletesSuccessfully_ItShouldCommit() {
	this.handle()

	this.So(this.closeCount, should.Equal, 2) // close connection and writer
	this.So(this.rollbackCount, should.Equal, 0)
	this.So(this.commitCount, should.Equal, 1)
	this.So(this.monitorCommitCount, should.Equal, 1)
	this.So(this.monitorRollbackCount, should.Equal, 0)
}
func (this *Fixture) TestWhenCommitOperationFails_ItShouldPanicAndRollBack() {
	this.commitError = errors.New("")

	this.So(this.handle, should.PanicWith, this.commitError)
	this.So(this.commitCount, should.Equal, 1)
	this.So(this.rollbackCount, should.Equal, 1)
	this.So(this.closeCount, should.Equal, 2) // close connection and writer
	this.So(this.monitorCommitErrors, should.Resemble, []error{this.commitError})
	this.So(this.monitorCommitCount, should.Equal, 0)
	this.So(this.monitorRollbackCount, should.Equal, 1)
}
func (this *Fixture) TestWhenCommitOperationFailsWithContextError_ItShouldPanicAndRollBackAndLogInfoMessage() {
	this.commitError = context.Canceled

	this.So(this.handle, should.PanicWith, this.commitError)
	this.So(this.commitCount, should.Equal, 1)
	this.So(this.rollbackCount, should.Equal, 1)
	this.So(this.closeCount, should.Equal, 2) // close connection and writer
	this.So(this.monitorCommitErrors, should.Resemble, []error{this.commitError})
	this.So(this.monitorCommitCount, should.Equal, 0)
	this.So(this.monitorRollbackCount, should.Equal, 1)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *Fixture) Connect(ctx context.Context) (messaging.Connection, error) {
	this.connectCount++
	this.connectContext = ctx
	return this, this.connectError
}
func (this *Fixture) Close() error {
	this.closeCount++
	return nil
}

func (this *Fixture) CommitWriter(ctx context.Context) (messaging.CommitWriter, error) {
	this.commitWriterCount++
	this.commitWriterContext = ctx
	ctx.(*transactionalContext).Store(this.sqlTx)
	return this, this.commitWriterError
}
func (this *Fixture) Reader(_ context.Context) (messaging.Reader, error) {
	panic("nop")
}
func (this *Fixture) Writer(_ context.Context) (messaging.Writer, error) {
	panic("nop")
}

func (this *Fixture) Commit() error {
	this.commitCount++
	return this.commitError
}
func (this *Fixture) Rollback() error {
	this.rollbackCount++
	return this.rollbackError
}
func (this *Fixture) Write(_ context.Context, _ ...messaging.Dispatch) (int, error) {
	panic("nop")
}

func (this *Fixture) factory(state State) messaging.Handler {
	this.factoryCount++
	this.factoryState = state
	if this.factoryError != nil {
		panic(this.factoryError)
	}
	return this
}
func (this *Fixture) Handle(ctx context.Context, messages ...any) {
	this.handleContext = ctx
	this.handleMessages = messages
	if this.handleError != nil {
		panic(this.handleError)
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type FakeMonitor struct{ fixture *Fixture }

func (this *FakeMonitor) TransactionStarted(err error) {
	this.fixture.monitorBeginErrors = append(this.fixture.monitorBeginErrors, err)
}
func (this *FakeMonitor) TransactionRolledBack(_ error) { this.fixture.monitorRollbackCount++ }
func (this *FakeMonitor) TransactionCommitted(err error) {
	if err == nil {
		this.fixture.monitorCommitCount++
	}

	this.fixture.monitorCommitErrors = append(this.fixture.monitorCommitErrors, err)
}
