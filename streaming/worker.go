package streaming

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/smarty/messaging/v4"
)

type defaultWorker struct {
	stream      messaging.Stream
	softContext context.Context
	hardContext context.Context
	readContext context.Context // child of hardContext; cancelled when this worker stops consuming
	cancelRead  context.CancelFunc
	handler     messaging.Handler
	logger      logger
	monitor     monitor
	now         func() time.Time
	streamName  string

	channelBuffer   chan messaging.Delivery
	currentBatch    []any
	unacknowledged  []messaging.Delivery
	handleDelivery  bool
	contextDelivery bool
	bufferTimeout   time.Duration
	strategy        ShutdownStrategy
	bufferLength    int
}

func newWorker(config workerConfig) messaging.Listener {
	readContext, cancelRead := context.WithCancel(config.HardContext)
	return &defaultWorker{
		stream:      config.Stream,
		softContext: config.SoftContext,
		hardContext: config.HardContext,
		readContext: readContext,
		cancelRead:  cancelRead,
		handler:     config.Handler,
		logger:      config.Logger,
		monitor:     config.Monitor,
		now:         config.Now,
		streamName:  config.Subscription.streamName,

		channelBuffer:   make(chan messaging.Delivery, config.Subscription.bufferCapacity),
		currentBatch:    make([]any, 0, config.Subscription.batchCapacity),
		unacknowledged:  make([]messaging.Delivery, 0, config.Subscription.batchCapacity),
		handleDelivery:  config.Subscription.handleDelivery,
		contextDelivery: config.Subscription.deliveryToContext,
		bufferTimeout:   config.Subscription.bufferTimeout,
		strategy:        config.Subscription.shutdownStrategy,
	}
}

func (this *defaultWorker) Listen() {
	var waiter sync.WaitGroup
	defer waiter.Wait()
	defer this.cancelRead() // runs before the wait: releases a reader parked in Read or on a full buffer
	defer this.reportPanic()

	waiter.Add(1)
	go this.readFromStream(&waiter)
	if this.handler == nil {
		waiter.Wait() // facilitates testing: let the reader run to completion and leave the buffer for inspection
		return
	}
	this.deliverToHandler()
}

// reportPanic makes an escaped handler panic attributable. Without it the
// runtime prints nothing until every deferred call returns, and before the
// worker-local read context existed, that was never.
func (this *defaultWorker) reportPanic() {
	if recovered := recover(); recovered != nil {
		this.logger.Printf("[ERROR] Handler on stream [%s] panicked [%v]; the worker is exiting.", this.streamName, recovered)
		panic(recovered)
	}
}

func (this *defaultWorker) readFromStream(waiter *sync.WaitGroup) {
	defer waiter.Done()
	defer close(this.channelBuffer)

	for {
		var delivery messaging.Delivery
		if err := this.stream.Read(this.readContext, &delivery); err != nil {
			this.logger.Printf("[INFO] Stream [%s] ended [%s].", this.streamName, err)
			return
		}

		select {
		case <-this.readContext.Done():
			return
		case this.channelBuffer <- delivery:
		}
	}
}
func (this *defaultWorker) deliverToHandler() {
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
		this.handleBatch()
	}

	err := this.stream.Acknowledge(this.hardContext, this.unacknowledged...)
	this.monitor.BatchAcknowledged(this.streamName, len(this.unacknowledged), err)
	if err != nil {
		this.logger.Printf("[WARN] Unable to acknowledge [%d] delivery(ies) from stream [%s] [%s]; the broker will redeliver them.",
			len(this.unacknowledged), this.streamName, err)
		return false
	}
	return true
}
func (this *defaultWorker) handleBatch() {
	started := this.now()
	defer func() { // deferred so a batch that panics is still measured
		this.monitor.BatchHandled(this.streamName, len(this.currentBatch), this.now().Sub(started))
	}()
	this.handler.Handle(this.deliveryContext(), this.currentBatch...)
}
func (this *defaultWorker) deliveryContext() context.Context {
	if this.contextDelivery {
		return context.WithValue(this.hardContext, ContextKeyDeliveries, this.unacknowledged)
	}

	return this.hardContext
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

var ContextKeyDeliveries = reflect.TypeOf([]messaging.Delivery{}).String()
