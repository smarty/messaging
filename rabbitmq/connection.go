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
	relay := make(chan amqp.Blocking, 1)
	go relayBlockedState(inner.NotifyBlocked(make(chan amqp.Blocking, 1)), relay)
	go this.watchBlockedState(relay)
	return this
}

// relayBlockedState keeps the amqp library's frame-dispatch goroutine from ever
// blocking on notification delivery: it drains promptly and, when the consumer
// lags (a slow monitor callback), keeps only the latest state.
// NOTE: the amqp library closes the notification channel when the connection closes.
func relayBlockedState(notifications, relay chan amqp.Blocking) {
	defer close(relay)
	for notification := range notifications {
		for delivered := false; !delivered; {
			select {
			case relay <- notification:
				delivered = true
			case <-relay: // discard the stale state; only the latest matters
			}
		}
	}
}
func (this *defaultConnection) watchBlockedState(notifications chan amqp.Blocking) {
	for notification := range notifications {
		if notification.Active {
			this.logger.Printf("[WARN] AMQP connection blocked by broker (reason: %s); publishes will stall until the broker unblocks.", notification.Reason)
			this.monitor.ConnectionBlocked(notification.Reason)
		} else {
			this.logger.Printf("[INFO] AMQP connection unblocked by broker; publishes resume.")
			this.monitor.ConnectionUnblocked()
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
