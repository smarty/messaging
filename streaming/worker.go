package streaming

import (
	"context"
	"sync"
	"time"

	"github.com/smartystreets/messaging/v3"
)

type defaultWorker struct {
	stream      messaging.Stream
	softContext context.Context
	hardContext context.Context
	handler     messaging.Handler

	channelBuffer  chan messaging.Delivery
	currentBatch   []interface{}
	unacknowledged []messaging.Delivery
	handleDelivery bool
	bufferTimeout  time.Duration
	strategy       ShutdownStrategy
	bufferLength   int
}

func newWorker(config workerConfig) messaging.Listener {
	return &defaultWorker{
		stream:      config.Stream,
		softContext: config.SoftContext,
		hardContext: config.HardContext,
		handler:     config.Handler,

		channelBuffer:  make(chan messaging.Delivery, config.Subscription.bufferCapacity),
		currentBatch:   make([]interface{}, 0, config.Subscription.batchCapacity),
		unacknowledged: make([]messaging.Delivery, 0, config.Subscription.batchCapacity),
		handleDelivery: config.Subscription.handleDelivery,
		bufferTimeout:  config.Subscription.bufferTimeout,
		strategy:       config.Subscription.shutdownStrategy,
	}
}

func (this *defaultWorker) Listen() {
	var waiter sync.WaitGroup
	defer waiter.Wait()

	waiter.Add(1)
	go this.readFromStream(&waiter)
	this.deliverToHandler()
}

func (this *defaultWorker) readFromStream(waiter *sync.WaitGroup) {
	defer waiter.Done()
	defer close(this.channelBuffer)

	for {
		var delivery messaging.Delivery
		if err := this.stream.Read(this.hardContext, &delivery); err != nil {
			break
		}

		select {
		case <-this.hardContext.Done():
			break
		case this.channelBuffer <- delivery:
		}
	}
}
func (this *defaultWorker) deliverToHandler() {
	if this.handler == nil {
		return // this facilitates testing
	}

	for delivery := range this.channelBuffer {
		if this.isComplete(ShutdownStrategyImmediate) {
			break
		}

		this.addToBatch(delivery)
		if this.canBatchMore() {
			continue
		}

		if !this.deliverBatch() {
			break
		}

		if this.isComplete(ShutdownStrategyCurrentBatch) {
			break
		}

		this.sleep()
		this.clearBatch()
	}
}

func (this *defaultWorker) addToBatch(delivery messaging.Delivery) {
	this.unacknowledged = append(this.unacknowledged, delivery)
	if delivery.Message == nil && !this.handleDelivery {
		return
	}

	if this.handleDelivery {
		this.currentBatch = append(this.currentBatch, delivery)
	} else {
		this.currentBatch = append(this.currentBatch, delivery.Message)
	}
}
func (this *defaultWorker) canBatchMore() bool {
	return this.measureBufferLength() > 0 && len(this.unacknowledged) < cap(this.unacknowledged)
}
func (this *defaultWorker) measureBufferLength() int {
	if this.bufferLength == 0 {
		this.bufferLength = len(this.channelBuffer)
	} else {
		this.bufferLength--
	}
	return this.bufferLength
}
func (this *defaultWorker) deliverBatch() bool {
	if len(this.currentBatch) > 0 {
		this.handler.Handle(this.hardContext, this.currentBatch...)
	}

	return this.stream.Acknowledge(this.hardContext, this.unacknowledged...) == nil
}
func (this *defaultWorker) clearBatch() {
	this.currentBatch = this.currentBatch[0:0]
	this.unacknowledged = this.unacknowledged[0:0]
}
func (this *defaultWorker) isComplete(strategy ShutdownStrategy) bool {
	return this.strategy == strategy && !isContextAlive(this.softContext)
}
func (this *defaultWorker) sleep() {
	if this.bufferTimeout <= 0 {
		return
	}

	if this.bufferLength > 0 {
		return // more work to do
	}

	wait, cancel := context.WithTimeout(this.softContext, this.bufferTimeout)
	defer cancel()
	<-wait.Done()
}
func isContextAlive(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	default:
		return true
	}
}
