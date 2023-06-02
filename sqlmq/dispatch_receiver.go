package sqlmq

import (
	"context"
	"database/sql"

	"github.com/smarty/messaging/v3"
	"github.com/smarty/messaging/v3/sqlmq/adapter"
)

type dispatchReceiver struct {
	ctx     context.Context
	tx      adapter.Transaction
	output  chan messaging.Dispatch
	store   messageStore
	logger  logger
	monitor monitor

	buffer []messaging.Dispatch
}

func newDispatchReceiver(ctx context.Context, tx adapter.Transaction, config configuration) messaging.CommitWriter {
	return &dispatchReceiver{
		ctx:     ctx,
		tx:      tx,
		output:  config.Channel,
		store:   config.MessageStore,
		logger:  config.Logger,
		monitor: config.Monitor,
	}
}

func (this *dispatchReceiver) Write(_ context.Context, dispatches ...messaging.Dispatch) (int, error) {
	this.buffer = append(this.buffer, dispatches...)
	length := len(dispatches)
	this.monitor.MessageReceived(length)
	return length, nil
}

func (this *dispatchReceiver) Commit() error {
	if err := this.store.Store(this.ctx, this.tx, this.buffer); err != nil {
		this.logger.Printf("[WARN] Unable to persist messages to durable storage [%s].", err)
		return err
	}

	if err := this.commit(); err == context.DeadlineExceeded {
		this.logger.Printf("[INFO] Unable to commit messages to durable storage [%s].", err)
		return err
	} else if err != nil {
		this.logger.Printf("[WARN] Unable to commit messages to durable storage [%s].", err)
		return err
	}

	this.monitor.MessageStored(len(this.buffer))
	for _, dispatch := range this.buffer {
		select {
		case this.output <- dispatch:
		case <-this.ctx.Done():
			return this.ctx.Err()
		}
	}

	// NOTE: we don't clear the buffer because the receiver is thrown away after commit
	return nil
}
func (this *dispatchReceiver) commit() error {
	err := this.tx.Commit()
	if err == sql.ErrTxDone && this.ctx.Err() == context.DeadlineExceeded {
		return context.DeadlineExceeded
	}

	return err
}

func (this *dispatchReceiver) Rollback() error { return this.tx.Rollback() }
func (this *dispatchReceiver) Close() error    { return nil }
