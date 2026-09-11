package status

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/smarty/messaging/v4"
)

type defaultStatusChecker struct {
	lock         *sync.Mutex
	logger       logger
	dispatch     messaging.Dispatch
	connector    messaging.Connector
	connection   messaging.Connection
	writer       messaging.CommitWriter
	tolerance    time.Duration
	now          func() time.Time
	firstFailure time.Time
}

func newDefaultStatusChecker(config configuration) Checker {
	return &defaultStatusChecker{
		lock:      new(sync.Mutex),
		logger:    config.logger,
		connector: config.connector,
		dispatch:  messaging.Dispatch{Topic: config.topic},
		tolerance: config.failureTolerance,
		now:       config.now,
	}
}
func (this *defaultStatusChecker) Status(ctx context.Context) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	err := this.tryWrite(ctx)
	if err == nil {
		this.firstFailure = time.Time{}
		return nil
	}
	if isDefinitive(err) {
		this.logger.Printf("[WARN] Status check failed with a definitive error (retries cannot succeed) [%s].", err)
		return err
	}
	if this.firstFailure.IsZero() {
		this.firstFailure = this.now()
	}
	if elapsed := this.now().Sub(this.firstFailure); elapsed < this.tolerance {
		this.logger.Printf("[WARN] Status check failed (tolerated; failing for %s of %s) [%s].", elapsed, this.tolerance, err)
		return nil
	}
	this.logger.Printf("[WARN] Status check failed [%s].", err)
	return err
}

// isDefinitive reports whether the error is a configuration fault that no
// retry can fix: bad credentials, a missing vhost, a denied permission, or a
// probe topic whose exchange does not exist. Such errors bypass the
// tolerance window.
func isDefinitive(err error) bool {
	var amqpError *amqp.Error
	if !errors.As(err, &amqpError) {
		return false
	}
	switch amqpError.Code {
	case amqp.AccessRefused, amqp.NotAllowed, amqp.NotFound:
		return true
	default:
		return false
	}
}

func (this *defaultStatusChecker) tryWrite(ctx context.Context) error {
	err := this.tryConnect(ctx)
	if err != nil {
		return err
	}
	err = this.write(ctx)
	if err != nil {
		_ = this.Close()
	}
	return err
}

// write bounds the probe with the caller's context. A broker that has stopped
// reading (a resource alarm) can block the underlying socket write
// indefinitely. On timeout, the checker severs the connection, which unblocks
// the write.
func (this *defaultStatusChecker) write(ctx context.Context) error {
	writer := this.writer
	completed := make(chan error, 1)
	go func() { completed <- this.probe(ctx, writer) }()
	select {
	case err := <-completed:
		return err
	case <-ctx.Done():
		_ = this.Close()
		<-completed // bounded: the severed connection errors the write promptly
		return ctx.Err()
	}
}

// probe publishes inside an AMQP transaction and commits. basic.publish is
// asynchronous, so a channel-level fault (403 on the exchange, 404 for a
// missing exchange) would otherwise arrive after the probe reported success
// and flap the tolerance window. tx.commit is synchronous: the broker answers
// commit-ok or closes the channel with the reason, and the writer's commit
// timeout bounds a stall on the probe topic.
func (this *defaultStatusChecker) probe(ctx context.Context, writer messaging.CommitWriter) (err error) {
	defer func() {
		// The rabbitmq writer panics on a topology error at commit when the
		// caller enables PanicOnTopologyError. A health probe reports; it must
		// never take the process down.
		if recovered := recover(); recovered != nil {
			err = asError(recovered)
		}
	}()
	if _, err = writer.Write(ctx, this.dispatch); err != nil {
		return err
	}
	return writer.Commit()
}
func asError(recovered any) error {
	if err, ok := recovered.(error); ok {
		return err
	}
	return fmt.Errorf("status probe panicked: %v", recovered)
}
func (this *defaultStatusChecker) tryConnect(ctx context.Context) (err error) {
	if this.connection != nil && this.writer != nil {
		return nil
	}
	this.connection, err = this.connector.Connect(ctx)
	if err != nil {
		return err
	}
	this.writer, err = this.connection.CommitWriter(ctx)
	if err != nil {
		_ = this.Close() // do not leak the dialed connection
		return err
	}
	return nil
}
func (this *defaultStatusChecker) Close() error {
	closeAll(this.connection, this.writer)
	this.connection, this.writer = nil, nil
	return nil
}
func closeAll(closers ...io.Closer) {
	for _, closer := range closers {
		if closer != nil {
			_ = closer.Close()
		}
	}
}
