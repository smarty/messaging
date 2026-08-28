package status

import (
	"context"
	"errors"
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
	writer       messaging.Writer
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
// retry can fix (bad credentials, missing vhost, denied permission); such
// errors bypass the failure-tolerance window.
func isDefinitive(err error) bool {
	var amqpError *amqp.Error
	if !errors.As(err, &amqpError) {
		return false
	}
	return amqpError.Code == amqp.AccessRefused || amqpError.Code == amqp.NotAllowed
}

func (this *defaultStatusChecker) tryWrite(ctx context.Context) error {
	err := this.tryConnect(ctx)
	if err != nil {
		return err
	}
	_, err = this.writer.Write(ctx, this.dispatch)
	if err != nil {
		_ = this.Close()
	}
	return err
}
func (this *defaultStatusChecker) tryConnect(ctx context.Context) (err error) {
	if this.connection != nil && this.writer != nil {
		return nil
	}
	this.connection, err = this.connector.Connect(ctx)
	if err != nil {
		return err
	}
	this.writer, err = this.connection.Writer(ctx)
	if err != nil {
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
