package transactional

import (
	"context"
	"database/sql"
	"io"

	"github.com/smarty/messaging/v4"
)

type handler struct {
	connector messaging.Connector
	factory   handlerFunc
	monitor   monitor
	logger    logger
}

func (this handler) Handle(ctx context.Context, messages ...interface{}) {
	if ctx == nil {
		panic(errNilContext)
	}

	connection, err := this.connector.Connect(ctx)
	if err != nil {
		this.logger.Printf("[WARN] Unable to begin transaction [%s].", err)
		this.monitor.TransactionStarted(err)
		panic(err)
	}

	txCtx := newContext(ctx, connection)
	defer func() { this.finally(txCtx, recover()) }()
	writer, err := connection.CommitWriter(txCtx)
	if err != nil {
		this.logger.Printf("[WARN] Unable to begin transaction [%s].", err)
		this.monitor.TransactionStarted(err)
		panic(err)
	}

	this.monitor.TransactionStarted(nil)
	txCtx.Writer = writer
	handler := this.factory(txCtx.State())
	handler.Handle(ctx, messages...)
	if err := writer.Commit(); err != nil {
		this.logger.Printf("[%s] Unable to commit transaction [%s].", logSeverity(err), err)
		this.monitor.TransactionCommitted(err)
		panic(err)
	}

	this.monitor.TransactionCommitted(nil)
}
func (this handler) finally(ctx *transactionalContext, err interface{}) {
	defer closeResource(ctx)
	if err == nil {
		return
	}

	if ctx.Writer != nil {
		this.monitor.TransactionRolledBack(ctx.Writer.Rollback())
	}

	panic(err)
}
func closeResource(resource io.Closer) {
	if resource != nil {
		_ = resource.Close()
	}
}
func logSeverity(err error) string {
	switch err {
	case context.Canceled, context.DeadlineExceeded:
		return "INFO"
	default:
		return "WARN"
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type transactionalContext struct {
	context.Context
	Tx         *sql.Tx
	Writer     messaging.CommitWriter
	Connection messaging.Connection
}

func newContext(ctx context.Context, connection messaging.Connection) *transactionalContext {
	return &transactionalContext{Context: ctx, Connection: connection}
}
func (this *transactionalContext) Store(tx *sql.Tx) { this.Tx = tx } // used by sqlmq
func (this *transactionalContext) State() State {
	return State{Tx: this.Tx, Writer: this.Writer}
}
func (this *transactionalContext) Close() error {
	if this.Writer != nil {
		_ = this.Writer.Close()
	}
	return this.Connection.Close()
}
