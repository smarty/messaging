package streaming

import (
	"context"
	"io"
	"sync"

	"github.com/smarty/messaging/v4"
)

type defaultSubscriber struct {
	pool         connectionPool
	subscription Subscription
	softContext  context.Context // pretty please be done as soon as possible.
	hardContext  context.Context // listen up, you're done RIGHT NOW!
	hardShutdown context.CancelFunc
	factory      workerFactory
	workersDone  chan struct{}
	logger       logger
}

func newSubscriber(pool connectionPool, subscription Subscription, softContext context.Context, factory workerFactory, logger logger) messaging.Listener {
	hardContext, hardShutdown := subscription.hardShutdown(softContext)
	return defaultSubscriber{
		pool:         pool,
		subscription: subscription,
		softContext:  softContext,
		hardContext:  hardContext,
		hardShutdown: hardShutdown,
		factory:      factory,
		workersDone:  make(chan struct{}),
		logger:       logger,
	}
}

func (this defaultSubscriber) Listen() {
	connection, err := this.pool.Active(this.softContext)
	if err != nil {
		this.logger.Printf("[WARN] Unable to open connection for stream [%s] [%s].", this.subscription.streamName, err)
		return
	}
	defer this.pool.Dispose(connection)

	reader, err := connection.Reader(this.softContext)
	if err != nil {
		this.logger.Printf("[WARN] Unable to open reader for stream [%s] [%s].", this.subscription.streamName, err)
		return
	}
	defer closeResource(reader)

	stream, err := reader.Stream(this.softContext, this.subscription.streamConfig())
	if err != nil {
		this.logger.Printf("[WARN] Unable to open stream [%s] [%s].", this.subscription.streamName, err)
		return
	}

	go this.listen(stream)
	this.shutdown(stream)
}
func (this defaultSubscriber) listen(stream messaging.Stream) {
	defer close(this.workersDone)

	var waiter sync.WaitGroup
	defer waiter.Wait()
	waiter.Add(len(this.subscription.handlers))

	for i := range this.subscription.handlers {
		go func(index int) {
			defer waiter.Done()
			this.consume(index, stream)
		}(i)
	}
}
func (this defaultSubscriber) consume(index int, stream messaging.Stream) {
	worker := this.factory(workerConfig{
		Stream:       stream,
		Subscription: this.subscription,
		Handler:      this.subscription.handlers[index],
		SoftContext:  this.softContext,
		HardContext:  this.hardContext,
		Logger:       this.logger,
	})
	worker.Listen()
}
func (this defaultSubscriber) shutdown(stream io.Closer) {
	select {
	case <-this.workersDone: // for some reason, workers have concluded before we expected
		closeResource(stream) // for example, the stream might have an error or the broker might have shut it down/terminated
	case <-this.softContext.Done():
		closeResource(stream) // now stop the stream from bringing in messages and give workers some time to conclude.
		deadline, cancel := context.WithTimeout(this.hardContext, this.subscription.shutdownTimeout)
		defer cancel()
		select {
		case <-this.workersDone:
			return // no need to wait for full deadline, workers have finished
		case <-deadline.Done():
			this.logger.Printf("[WARN] Workers on stream [%s] did not conclude within [%s] of shutdown; abandoning in-flight deliveries.",
				this.subscription.streamName, this.subscription.shutdownTimeout)
			this.hardShutdown() // tell workers to stop, they're taking too long
			<-this.workersDone
		}
	}
}
