package sqlmq

import (
	"context"

	"github.com/smartystreets/messaging/v4"
)

type defaultConnector struct{ config configuration }

func newConnector(config configuration) messaging.Connector {
	return defaultConnector{config: config}
}
func (this defaultConnector) Connect(_ context.Context) (messaging.Connection, error) {
	return defaultConnection{config: this.config}, nil
}
func (this defaultConnector) Close() error {
	return this.config.StorageHandle.Close()
}

type defaultConnection struct{ config configuration }

func (this defaultConnection) Reader(_ context.Context) (messaging.Reader, error) {
	panic("not supported")
}
func (this defaultConnection) Writer(_ context.Context) (messaging.Writer, error) {
	panic("not supported")
}
func (this defaultConnection) CommitWriter(ctx context.Context) (messaging.CommitWriter, error) {
	tx, err := this.config.StorageHandle.BeginTx(ctx, &this.config.SQLTxOptions)
	if err != nil {
		this.config.Logger.Printf("[WARN] Unable to begin new storage transaction [%s].", err)
		return nil, err
	}

	if txCtx, ok := ctx.(transactionalContext); ok {
		txCtx.Store(tx.TxHandle())
	}

	return newDispatchReceiver(ctx, tx, this.config), nil
}

func (this defaultConnection) Close() error { return nil }
