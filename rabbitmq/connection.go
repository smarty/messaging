package rabbitmq

import (
	"context"
	"sync"

	"github.com/smarty/messaging/v3"
	"github.com/smarty/messaging/v3/rabbitmq/adapter"
)

type defaultConnection struct {
	inner   adapter.Connection
	config  configuration
	logger  logger
	monitor monitor
	closer  sync.Once
}

func newConnection(inner adapter.Connection, config configuration) messaging.Connection {
	// NOTE: using pointer type to allow for pointer equality check
	config.Monitor.ConnectionOpened(nil)
	return &defaultConnection{inner: inner, config: config, logger: config.Logger, monitor: config.Monitor}
}
func (this *defaultConnection) Reader(_ context.Context) (messaging.Reader, error) {
	if channel, err := this.inner.Channel(); err != nil {
		this.logger.Printf("[WARN] Unable able open read channel [%s].", err)
		return nil, err
	} else {
		return newReader(channel, this.config), nil
	}
}

func (this *defaultConnection) Writer(_ context.Context) (messaging.Writer, error) {
	return this.writer(false)
}
func (this *defaultConnection) CommitWriter(_ context.Context) (messaging.CommitWriter, error) {
	return this.writer(true)
}
func (this *defaultConnection) writer(transactional bool) (messaging.CommitWriter, error) {
	channel, err := this.inner.Channel()
	if err != nil {
		this.logger.Printf("[WARN] Unable able open write channel [%s].", err)
		return nil, err
	}

	if !transactional {
		return newWriter(channel, this.config), nil
	}

	if err = channel.Tx(); err != nil {
		_ = channel.Close()
		return nil, err
	}

	return newWriter(channel, this.config), nil
}

func (this *defaultConnection) Close() (err error) {
	this.closer.Do(func() {
		err = this.inner.Close()
		this.monitor.ConnectionClosed()
	})

	return err
}
