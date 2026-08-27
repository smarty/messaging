package rabbitmq

import (
	"context"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/smarty/messaging/v4"
	"github.com/smarty/messaging/v4/rabbitmq/adapter"
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
	this := &defaultConnection{inner: inner, config: config, logger: config.Logger, monitor: config.Monitor}
	go this.watchBlockedState(inner.NotifyBlocked(make(chan amqp.Blocking, 1)))
	return this
}
func (this *defaultConnection) watchBlockedState(notifications chan amqp.Blocking) {
	// NOTE: the amqp library closes the notification channel when the connection closes.
	blocked, _ := this.monitor.(blockedMonitor)
	for notification := range notifications {
		if notification.Active {
			this.logger.Printf("[WARN] AMQP connection blocked by broker (reason: %s); publishes will stall until the broker unblocks.", notification.Reason)
			if blocked != nil {
				blocked.ConnectionBlocked(notification.Reason)
			}
		} else {
			this.logger.Printf("[INFO] AMQP connection unblocked by broker; publishes resume.")
			if blocked != nil {
				blocked.ConnectionUnblocked()
			}
		}
	}
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
