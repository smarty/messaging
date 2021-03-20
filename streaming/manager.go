package streaming

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/smartystreets/messaging/v3"
)

type subscriberFactory func(context.Context, Subscription) messaging.Listener
type defaultManager struct {
	softContext    context.Context
	softShutdown   context.CancelFunc
	subscriptions  []Subscription
	connectionPool io.Closer
	factory        subscriberFactory
}

func newManager(pool io.Closer, subscriptions []Subscription, factory subscriberFactory) messaging.ListenCloser {
	softContext, softShutdown := context.WithCancel(context.Background())
	return defaultManager{
		softContext:    softContext,
		softShutdown:   softShutdown,
		subscriptions:  subscriptions,
		connectionPool: pool,
		factory:        factory,
	}
}

func (this defaultManager) Listen() {
	defer closeResource(this.connectionPool)

	var waiter sync.WaitGroup
	waiter.Add(len(this.subscriptions))
	defer waiter.Wait()

	for i := range this.subscriptions {
		go func(index int) {
			defer waiter.Done()
			this.listen(index)
		}(i)
	}
}
func (this defaultManager) listen(index int) {
	subscription := this.subscriptions[index]
	for this.isAlive() {
		subscriber := this.factory(this.softContext, subscription)
		subscriber.Listen()
		this.sleep(subscription.reconnectDelay)
	}
}
func (this defaultManager) isAlive() bool {
	select {
	case <-this.softContext.Done():
		return false
	default:
		return true
	}
}
func (this defaultManager) sleep(duration time.Duration) {
	if duration == 0 {
		return
	}

	sleeper, cancel := context.WithTimeout(this.softContext, duration)
	defer cancel()
	<-sleeper.Done()
}

func (this defaultManager) Close() error {
	this.softShutdown()
	return nil
}
func closeResource(resource io.Closer) {
	if resource != nil {
		_ = resource.Close()
	}
}
