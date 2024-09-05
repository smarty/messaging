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
func (this *defaultStatusChecker) Status(ctx context.Context) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	err := this.tryWrite(ctx)
	if err == nil {
		return nil
	}
	if strings.Contains(strings.ToLower(err.Error()), "password") {
		return err
	}
	return nil
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
