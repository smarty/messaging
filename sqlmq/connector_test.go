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

func TestConnectorFixture(t *testing.T) {
	gunit.Run(new(ConnectorFixture), t)
}

type ConnectorFixture struct {
	*gunit.Fixture

	connector messaging.Connector

	ctx        context.Context
	sqlOptions sql.TxOptions
	sqlTx      *sql.Tx

	closeError   error
	beginContext context.Context
	beginOptions sql.TxOptions
	beginError   error
}

func (this *ConnectorFixture) Setup() {
	config := configuration{}
	Options.apply(
		Options.Context(context.Background()),
		Options.IsolationLevel(sql.LevelReadCommitted),
		Options.StorageHandle(&sql.DB{}),
	)(&config)
	this.ctx = config.Context
	this.sqlOptions = config.SQLTxOptions
	this.sqlTx = &sql.Tx{}
	config.StorageHandle = this
	this.connector = newConnector(config)
}

func (this *ConnectorFixture) TestWhenClosing_ItShouldCloseUnderlyingHandle() {
	this.closeError = errors.New("")

	err := this.connector.Close()

	this.So(err, should.Equal, this.closeError)
}
func (this *ConnectorFixture) TestWhenClosingCreatedConnection_Nop() {
	this.closeError = errors.New("")

	connection, _ := this.connector.Connect(this.ctx)
	err := connection.Close()

	this.So(err, should.BeNil)
}

func (this *ConnectorFixture) TestWhenOpeningAReader_ItShouldPanic() {
	connection, err := this.connector.Connect(this.ctx)

	this.So(connection, should.NotBeNil)
	this.So(err, should.BeNil)
	this.So(func() { _, _ = connection.Reader(this.ctx) }, should.Panic)
}
func (this *ConnectorFixture) TestWhenOpeningARegularWriter_ItShouldPanic() {
	connection, err := this.connector.Connect(this.ctx)

	this.So(connection, should.NotBeNil)
	this.So(err, should.BeNil)
	this.So(func() { _, _ = connection.Writer(this.ctx) }, should.Panic)
}

func (this *ConnectorFixture) TestWhenOpeningCommitWriterAndNewTxFails_ItShouldReturnError() {
	this.beginError = errors.New("")

	connection, _ := this.connector.Connect(this.ctx)
	writer, err := connection.CommitWriter(this.ctx)

	this.So(writer, should.BeNil)
	this.So(err, should.Equal, this.beginError)
	this.So(this.beginContext, should.Equal, this.ctx)
	this.So(this.beginOptions, should.Resemble, this.sqlOptions)
}
func (this *ConnectorFixture) TestWhenOpeningCommitWriterItShouldReturnNewWriter() {
	connection, _ := this.connector.Connect(this.ctx)

	writer, err := connection.CommitWriter(this.ctx)

	this.So(writer, should.HaveSameTypeAs, &dispatchReceiver{})
	this.So(err, should.BeNil)
}
func (this *ConnectorFixture) TestWhenOpeningCommitWriterWithTxStateOnCallingContext_ItShouldAppendTxToContext() {
	var state TxState
	contextWithTxState := context.WithValue(this.ctx, txStateContextKey, &state)
	connection, _ := this.connector.Connect(this.ctx)

	_, _ = connection.CommitWriter(contextWithTxState)

	this.So(state.Tx, should.Equal, this.sqlTx)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *ConnectorFixture) Close() error { return this.closeError }

func (this *ConnectorFixture) QueryContext(ctx context.Context, statement string, args ...interface{}) (adapter.QueryResult, error) {
	panic("nop")
}
func (this *ConnectorFixture) QueryRowContext(ctx context.Context, statement string, args ...interface{}) adapter.RowScanner {
	panic("nop")
}
func (this *ConnectorFixture) ExecContext(ctx context.Context, statement string, args ...interface{}) (sql.Result, error) {
	panic("nop")
}
func (this *ConnectorFixture) BeginTx(ctx context.Context, options *sql.TxOptions) (adapter.Transaction, error) {
	this.beginContext = ctx
	this.beginOptions = *options
	return this, this.beginError
}
func (this *ConnectorFixture) Commit() error {
	panic("implement me")
}
func (this *ConnectorFixture) Rollback() error {
	panic("implement me")
}
func (this *ConnectorFixture) DBHandle() *sql.DB {
	panic("nop")
}
func (this *ConnectorFixture) TxHandle() *sql.Tx { return this.sqlTx }
