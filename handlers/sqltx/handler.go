package sqltx

import (
	"context"
	"database/sql"

	"github.com/smartystreets/messaging/v3"
)

type handler struct {
	logger    logger
	monitor   monitor
	txOptions *sql.TxOptions
	handle    *sql.DB
	callback  handlerFunc
}

func newHandler(config configuration) messaging.Handler {
	return &handler{
		logger:    config.Logger,
		monitor:   config.Monitor,
		txOptions: &sql.TxOptions{Isolation: config.IsolationLevel, ReadOnly: config.ReadOnly},
		handle:    config.Handle,
		callback:  config.Callback,
	}
}

func (this *handler) Handle(ctx context.Context, messages ...any) {
	tx := this.beginTransaction(ctx)

	defer func() { this.finally(tx, recover()) }()
	this.callback(tx).Handle(ctx, messages...)
}
func (this *handler) beginTransaction(ctx context.Context) *sql.Tx {
	if tx, err := this.handle.BeginTx(ctx, this.txOptions); err != nil {
		this.logger.Printf("[WARN] Unable to begin transaction [%s].", err)
		this.monitor.TransactionStarted(err)
		panic(err)
	} else {
		this.monitor.TransactionStarted(nil)
		return tx
	}
}
func (this *handler) finally(tx *sql.Tx, recovered any) {
	if recovered != nil {
		_ = tx.Rollback()
		panic(recovered)
	} else if err := tx.Commit(); err != nil {
		this.logger.Printf("[%s] Unable to commit transaction [%s].", logSeverity(err), err)
		this.monitor.TransactionCommitted(err)
		_ = tx.Rollback()
		panic(err)
	}

	this.monitor.TransactionCommitted(nil)
}
func logSeverity(err error) string {
	switch err {
	case context.Canceled, context.DeadlineExceeded:
		return "INFO"
	default:
		return "WARN"
	}
}
