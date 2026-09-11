package sqlmq

import (
	"context"
	"database/sql"
	"time"

	"github.com/smarty/messaging/v4"
	"github.com/smarty/messaging/v4/sqlmq/adapter"
)

type dispatchReceiver struct {
	ctx            context.Context
	tx             adapter.Transaction
	output         chan messaging.Dispatch
	store          messageStore
	logger         logger
	monitor        monitor
	handoffTimeout time.Duration
	deferred       *deferredHandoffs

	buffer []messaging.Dispatch
}

func newDispatchReceiver(ctx context.Context, tx adapter.Transaction, config configuration) messaging.CommitWriter {
	return &dispatchReceiver{
		ctx:            ctx,
		tx:             tx,
		output:         config.Channel,
		store:          config.MessageStore,
		logger:         config.Logger,
		monitor:        config.Monitor,
		handoffTimeout: config.HandoffTimeout,
		deferred:       config.Deferred,
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

	// From here on the rows are durable, so every path returns nil: an error
	// would make the caller retry a batch whose side effects already happened.
	this.monitor.MessageStored(len(this.buffer))
	remaining := this.handoff(this.buffer, this.handoffTimeout)
	if len(remaining) == 0 {
		return nil
	}
	if this.ctx.Err() != nil {
		this.logger.Printf("[INFO] Context ended during handoff; [%d] committed message(s) remain in durable storage for the next startup.", len(remaining))
		return nil
	}
	if this.deferred.TryDefer(this.ctx, remaining) {
		this.logger.Printf("[WARN] Committed [%d] message(s) to durable storage, but the dispatch processor did not accept [%d] of them within [%s]. The handoff continues in the background.",
			len(this.buffer), len(remaining), this.handoffTimeout)
		return nil
	}
	this.logger.Printf("[WARN] Deferred handoff capacity [%d] reached; waiting for the dispatch processor to accept [%d] message(s).",
		this.deferred.capacity, len(remaining))
	this.handoff(remaining, 0) // no timer: wait for the channel or for shutdown

	// NOTE: we don't clear the buffer because the receiver is thrown away after commit
	return nil
}

// handoff sends dispatches into the output channel until all are accepted,
// the context ends, or the timeout elapses (a zero timeout means no timer).
// It returns the dispatches that were not accepted.
func (this *dispatchReceiver) handoff(dispatches []messaging.Dispatch, timeout time.Duration) (remaining []messaging.Dispatch) {
	var expired <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		expired = timer.C
	}
	for index, dispatch := range dispatches {
		select {
		case this.output <- dispatch:
		case <-this.ctx.Done():
			return dispatches[index:]
		case <-expired:
			return dispatches[index:]
		}
	}
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
