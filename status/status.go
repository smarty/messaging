package status

import (
	"context"
	"io"
	"strings"
	"sync"

	"github.com/smarty/messaging/v3"
)

type defaultStatusChecker struct {
	lock       *sync.Mutex
	logger     logger
	dispatch   messaging.Dispatch
	connector  messaging.Connector
	connection messaging.Connection
	writer     messaging.Writer
}

func newDefaultStatusChecker(config configuration) Checker {
	return &defaultStatusChecker{
		lock:      new(sync.Mutex),
		logger:    config.logger,
		connector: config.connector,
		dispatch:  messaging.Dispatch{Topic: config.topic},
	}
}
func (this *defaultStatusChecker) Status(ctx context.Context) (err error) {
	this.lock.Lock()
	defer this.lock.Unlock()
	defer func() {
		if err != nil && !strings.Contains(strings.ToLower(err.Error()), "password") {
			err = nil
		}
	}()
	err = this.connect(ctx)
	if err != nil {
		return err
	}
	_, err = this.writer.Write(ctx, this.dispatch)
	if err != nil {
		_ = this.Close()
	}
	return err
}
func (this *defaultStatusChecker) connect(ctx context.Context) (err error) {
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
