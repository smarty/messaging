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
	done    chan struct{}
	closer  sync.Once
}

func newConnection(inner adapter.Connection, config configuration) messaging.Connection {
	// NOTE: using pointer type to allow for pointer equality check
	config.Monitor.ConnectionOpened(nil)
	this := &defaultConnection{inner: inner, config: config, logger: config.Logger, monitor: config.Monitor, done: make(chan struct{})}
	relay := make(chan amqp.Blocking, 1)
	go relayBlockedState(inner.BlockedNotifications(), relay, this.done)
	go this.watchBlockedState(relay)
	return this
}

// relayBlockedState keeps the amqp library's frame-dispatch goroutine from
// ever blocking on notification delivery. It drains promptly and, when the
// consumer lags (a slow monitor callback), keeps only the latest state.
// It exits when the amqp library closes the notification channel or when done
// closes. An adapter.Connection implementation that never closes the channel
// therefore cannot leak the goroutine. Closing relay ends the watcher in turn.
func relayBlockedState(notifications <-chan amqp.Blocking, relay chan amqp.Blocking, done chan struct{}) {
	defer close(relay)
	for {
		select {
		case <-done:
			return
		case notification, open := <-notifications:
			if !open || !deliverLatest(notification, relay, done) {
				return
			}
		}
	}
}
func deliverLatest(notification amqp.Blocking, relay chan amqp.Blocking, done chan struct{}) bool {
	select {
	case <-done: // checked first: never deliver after the connection closes
		return false
	default:
	}
	for {
		select {
		case <-done:
			return false
		case relay <- notification:
			return true
		case <-relay: // discard the stale state; only the latest matters
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
		return newReader(channel, this.Close, this.config), nil
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
		return newWriter(channel, this.Close, this.config), nil
	}

	if err = channel.Tx(); err != nil {
		_ = channel.Close()
		return nil, err
	}

	return newWriter(channel, this.Close, this.config), nil
}

// Closed reports whether this connection has been closed, by its owner or by
// a writer that severed it after a commit timeout. Consumers that cache a
// shared connection use it to avoid handing out a dead one.
func (this *defaultConnection) Closed() bool {
	select {
	case <-this.done:
		return true
	default:
		return false
	}
}

func (this *defaultConnection) Close() (err error) {
	this.closer.Do(func() {
		close(this.done)
		err = this.inner.Close()
		this.monitor.ConnectionClosed()
	})

	return err
}
