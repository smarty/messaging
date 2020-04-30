package streaming

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/messaging/v3"
)

func TestWorkerFixture(t *testing.T) {
	gunit.Run(new(WorkerFixture), t)
}

type WorkerFixture struct {
	*gunit.Fixture

	handler       messaging.Handler
	softContext   context.Context
	softShutdown  context.CancelFunc
	hardContext   context.Context
	hardShutdown  context.CancelFunc
	subscription  Subscription
	channelBuffer chan messaging.Delivery
	worker        messaging.Listener

	readCount      int
	maxReadCount   int
	readContext    context.Context
	readDeliveries []messaging.Delivery
	readError      error

	acknowledgeTimestamp  time.Time
	acknowledgeCount      int
	acknowledgeContext    context.Context
	acknowledgeDeliveries []messaging.Delivery
	acknowledgeError      error

	closeCount int

	handleTimestamp time.Time
	handleCount     int
	handleCtx       context.Context
	handleMessages  []interface{}
}

func (this *WorkerFixture) Setup() {
	this.handler = this
	this.softContext, this.softShutdown = context.WithCancel(context.Background())
	this.hardContext, this.hardShutdown = context.WithCancel(context.Background())
	this.subscription = Subscription{BufferSize: 16, MaxBatchSize: 16}
	this.initializeWorker()
}
func (this *WorkerFixture) initializeWorker() {
	worker := newWorker(workerConfig{
		Stream:       this,
		Subscription: this.subscription,
		Handler:      this.handler,
		SoftContext:  this.softContext,
		HardContext:  this.hardContext,
	}).(*defaultWorker)
	this.worker = worker
	this.channelBuffer = worker.channelBuffer
}

func (this *WorkerFixture) TestWhenNewWorkerCreated_UnderlyingBufferShouldComeFromSubscription() {
	this.So(cap(this.channelBuffer), should.Equal, this.subscription.BufferSize)
}

func (this *WorkerFixture) TestWhenReadingFromUnderlyingStream_AddToBufferedChannelUntilReadFailure() {
	this.handler = nil
	this.readError = io.EOF
	this.maxReadCount = cap(this.channelBuffer)
	this.initializeWorker()

	this.worker.Listen()

	this.So(len(this.channelBuffer), should.Equal, cap(this.channelBuffer)) // buffer is full
	this.So(this.readContext, should.Equal, this.hardContext)
	this.So(this.readCount, should.Equal, cap(this.channelBuffer)+1) // last read fails

	var readDeliveries []messaging.Delivery
	for delivery := range this.channelBuffer {
		readDeliveries = append(readDeliveries, delivery)
	}
	this.So(this.readDeliveries, should.Resemble, readDeliveries) // the buffered deliveries are the ones that were read

	_, open := <-this.channelBuffer
	this.So(open, should.BeFalse) // channel closed when read completed
}
func (this *WorkerFixture) TestWhenReadingFromUnderlyingStream_FailOnContextClosure() {
	this.hardShutdown()

	this.worker.Listen()

	this.So(this.channelBuffer, should.BeEmpty)
	this.So(this.readContext, should.Equal, this.hardContext)
	this.So(this.readCount, should.Equal, 1)
}
func (this *WorkerFixture) TestWhenChannelBufferIsFull_WaitUntilSpaceAvailableOrContextCancellation() {
	this.handler = nil // don't delivery to handler
	this.maxReadCount = cap(this.channelBuffer)
	this.initializeWorker()

	this.worker.Listen()

	this.So(len(this.channelBuffer), should.Equal, cap(this.channelBuffer)) // buffer is full
	this.So(this.readContext, should.Equal, this.hardContext)
}

func (this *WorkerFixture) TestWhenOnlySingleDeliveryAvailable_SendTheBatchWithoutWaitingForMore() {
	this.readError = io.EOF
	this.channelBuffer <- messaging.Delivery{Message: 1}

	this.worker.Listen()

	this.So(this.handleCtx, should.Equal, this.hardContext)
	this.So(this.handleCount, should.Equal, 1)
	this.So(this.handleMessages, should.Resemble, []interface{}{1})

	this.So(this.acknowledgeContext, should.Equal, this.hardContext)
	this.So(this.acknowledgeCount, should.Equal, 1)
	this.So(this.acknowledgeDeliveries, should.Resemble, []messaging.Delivery{{Message: 1}})
}
func (this *WorkerFixture) TestWhenStreamingToHandler_BatchOfMessagesPushedToHandlersAndDeliveriesAcknowledged() {
	this.readError = io.EOF
	deliveries := []messaging.Delivery{{Message: 1}, {Message: 2}, {Message: 3}}
	for _, delivery := range deliveries {
		this.channelBuffer <- delivery
	}

	this.worker.Listen()

	this.So(this.handleCtx, should.Equal, this.hardContext)
	this.So(this.handleCount, should.Equal, 1)
	this.So(this.handleMessages, should.Resemble, []interface{}{1, 2, 3})

	this.So(this.acknowledgeContext, should.Equal, this.hardContext)
	this.So(this.acknowledgeCount, should.Equal, 1)
	this.So(this.acknowledgeDeliveries, should.Resemble, deliveries)
}
func (this *WorkerFixture) TestWhenMoreDeliveriesExistThanBatchMax_DeliverInBatchesOfMaxSpecifiedSize() {
	this.subscription.MaxBatchSize = 2
	this.subscription.BufferSize = 5
	this.initializeWorker()
	this.readError = io.EOF
	deliveries := []messaging.Delivery{{Message: 1}, {Message: 2}, {Message: 3}, {Message: 4}, {Message: 5}}
	for _, delivery := range deliveries {
		this.channelBuffer <- delivery
	}

	this.worker.Listen()

	this.So(this.handleCtx, should.Equal, this.hardContext)
	this.So(this.handleCount, should.Equal, 3)
	this.So(this.handleMessages, should.Resemble, []interface{}{1, 2, 3, 4, 5})

	this.So(this.acknowledgeContext, should.Equal, this.hardContext)
	this.So(this.acknowledgeCount, should.Equal, 3)
	this.So(this.acknowledgeDeliveries, should.Resemble, deliveries)
}
func (this *WorkerFixture) TestWhenConfigured_PassFullDeliveryToHandler() {
	this.subscription.HandleDelivery = true
	this.initializeWorker()
	this.readError = io.EOF
	this.channelBuffer <- messaging.Delivery{Message: 1}

	this.worker.Listen()

	this.So(this.handleCtx, should.Equal, this.hardContext)
	this.So(this.handleCount, should.Equal, 1)
	this.So(this.handleMessages, should.Resemble, []interface{}{messaging.Delivery{Message: 1}})
}

func (this *WorkerFixture) TestWhenAcknowledgementFails_ListeningConcludesWithoutProcessingBufferedDeliveries() {
	this.acknowledgeError = errors.New("")
	this.readError = io.EOF
	this.subscription.MaxBatchSize = 1
	this.subscription.BufferSize = 2
	this.initializeWorker()
	this.channelBuffer <- messaging.Delivery{Message: 1}
	this.channelBuffer <- messaging.Delivery{Message: 2}

	this.worker.Listen()

	this.So(this.acknowledgeCount, should.Equal, 1)
	this.So(len(this.channelBuffer), should.Equal, 1)
}
func (this *WorkerFixture) TestWhenConfiguredToBufferBetweenBatches_SleepAfterAcknowledgement() {
	const timeout = time.Millisecond * 5
	this.readError = io.EOF
	this.subscription.BufferTimeout = timeout
	this.initializeWorker()

	this.channelBuffer <- messaging.Delivery{Message: 1}

	this.worker.Listen()

	this.So(this.acknowledgeTimestamp, should.HappenBefore, time.Now().Add(-timeout))
}

func (this *WorkerFixture) TestWhenRequestingShutdownAndStrategyIsImmediate_DoNotDeliveryMoreToHandler() {
	this.readError = io.EOF
	this.subscription.ShutdownStrategy = ShutdownStrategyImmediate
	this.initializeWorker()
	this.channelBuffer <- messaging.Delivery{Message: 1}

	this.softShutdown()
	this.worker.Listen()

	this.So(this.handleCount, should.Equal, 0)
}
func (this *WorkerFixture) TestWhenRequestingShutdownAndStrategyIsCurrentBatch_DeliverAndAckBatchThenExit() {
	this.readError = io.EOF
	this.subscription.MaxBatchSize = 2
	this.subscription.BufferTimeout = time.Second // should not be reached
	this.subscription.ShutdownStrategy = ShutdownStrategyCurrentBatch
	this.initializeWorker()
	this.channelBuffer <- messaging.Delivery{Message: 1}
	this.channelBuffer <- messaging.Delivery{Message: 2}
	this.channelBuffer <- messaging.Delivery{Message: 3}

	this.softShutdown()
	this.worker.Listen()

	this.So(this.handleCount, should.Equal, 1)
	this.So(this.acknowledgeCount, should.Equal, 1)
	this.So(this.acknowledgeTimestamp, should.HappenWithin, time.Millisecond*25, time.Now()) // 1-second sleep is skipped
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *WorkerFixture) Read(ctx context.Context, delivery *messaging.Delivery) error {
	this.readCount++
	this.readContext = ctx

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if this.readCount > this.maxReadCount && this.readError != nil {
		return this.readError
	} else if this.readCount > this.maxReadCount {
		this.hardShutdown()
	}

	delivery.MessageID = uint64(this.readCount)
	this.readDeliveries = append(this.readDeliveries, *delivery)
	return nil
}
func (this *WorkerFixture) Acknowledge(ctx context.Context, deliveries ...messaging.Delivery) error {
	this.acknowledgeTimestamp = time.Now()
	this.acknowledgeCount++
	this.acknowledgeContext = ctx
	this.acknowledgeDeliveries = append(this.acknowledgeDeliveries, deliveries...)
	return this.acknowledgeError
}
func (this *WorkerFixture) Close() error { panic("nop") }

func (this *WorkerFixture) Handle(ctx context.Context, messages ...interface{}) {
	this.handleTimestamp = time.Now()
	this.handleCount++
	this.handleCtx = ctx
	this.handleMessages = append(this.handleMessages, messages...)
}
