package streaming

import (
	"context"
	"sync"

	"github.com/smartystreets/messaging/v3"
)

type defaultSubscriber struct {
	pool         connectionPool
	subscription Subscription
	softContext  context.Context // pretty please be done as soon as possible.
	hardContext  context.Context // listen up, you're done RIGHT NOW!
	hardShutdown context.CancelFunc
	factory      workerFactory
	workersDone  chan struct{}
}

func newSubscriber(pool connectionPool, subscription Subscription, softContext context.Context, factory workerFactory) messaging.Listener {
	hardContext, hardShutdown := subscription.hardShutdown(softContext)
	return defaultSubscriber{
		pool:         pool,
		subscription: subscription,
		softContext:  softContext,
		hardContext:  hardContext,
		hardShutdown: hardShutdown,
		factory:      factory,
		workersDone:  make(chan struct{}),
	}
}

func (this defaultSubscriber) Listen() {
	connection, err := this.pool.Active(this.softContext)
	if err != nil {
		return
	}
	defer this.pool.Dispose(connection)

	reader, err := connection.Reader(this.softContext)
	if err != nil {
		return
	}
	defer closeResource(reader)

	var streams []messaging.Stream
	for _, streamConfig := range this.subscription.streamConfigs {
		if stream, err := reader.Stream(this.softContext, streamConfig); err == nil {
			streams = append(streams, stream)
		} else {
			closeStreams(streams)
			return
		}
	}

	go this.listen(streams)
	this.shutdown(streams)
}
func (this defaultSubscriber) listen(streams []messaging.Stream) {
	defer close(this.workersDone)

	var waiter sync.WaitGroup
	defer waiter.Wait()
	waiter.Add(len(this.subscription.handlers))

	for i := range this.subscription.handlers {
		go func(index int) {
			defer waiter.Done()
			this.consume(index, streams)
		}(i)
	}
}
func (this defaultSubscriber) consume(index int, streams []messaging.Stream) {
	worker := this.factory(workerConfig{
		Streams:      streams,
		Subscription: this.subscription,
		Handler:      this.subscription.handlers[index],
		SoftContext:  this.softContext,
		HardContext:  this.hardContext,
	})
	worker.Listen()
}
func (this defaultSubscriber) shutdown(streams []messaging.Stream) {
	select {
	case <-this.workersDone: // for some reason, workers have concluded before we expected
		closeStreams(streams) // for example, the stream might have an error or the broker might have shut it down/terminated
	case <-this.softContext.Done():
		closeStreams(streams) // now stop the stream from bringing in messages and give workers some time to conclude.
		deadline, cancel := context.WithTimeout(this.hardContext, this.subscription.shutdownTimeout)
		defer cancel()
		select {
		case <-this.workersDone:
			return // no need to wait for full deadline, workers have finished
		case <-deadline.Done():
			this.hardShutdown() // tell workers to stop, they're taking too long
			<-this.workersDone
		}
	}
}
func closeStreams(streams []messaging.Stream) {
	for _, stream := range streams {
		closeResource(stream)
	}
}
