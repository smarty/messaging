package streaming

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/smarty/messaging/v3"
	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
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

	acknowledgeTimestamp  []time.Time
	acknowledgeCount      int
	acknowledgeContext    context.Context
	acknowledgeDeliveries []messaging.Delivery
	acknowledgeError      error

	closeCount int

	handleTimestamp time.Time
	handleCount     int
	handleCtx       context.Context
	handleMessages  []any
}

func (this *WorkerFixture) Setup() {
	this.handler = this
	this.softContext, this.softShutdown = context.WithCancel(context.Background())
	this.hardContext, this.hardShutdown = context.WithCancel(context.Background())
	this.subscription = Subscription{bufferCapacity: 16, batchCapacity: 16}
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
	this.So(cap(this.channelBuffer), should.Equal, this.subscription.bufferCapacity)
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
	this.So(this.handleMessages, should.Resemble, []any{1})

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
	this.So(this.handleMessages, should.Resemble, []any{1, 2, 3})

	this.So(this.acknowledgeContext, should.Equal, this.hardContext)
	this.So(this.acknowledgeCount, should.Equal, 1)
	this.So(this.acknowledgeDeliveries, should.Resemble, deliveries)
}
func (this *WorkerFixture) TestWhenStreamingToHandler_DoNotPushDeliveriesWithBlankMessage_ButStillAckAllDeliveries() {
	this.readError = io.EOF
	deliveries := []messaging.Delivery{{Message: 1}, { /* delivery nil/blank message */ }, {Message: 3}, { /* delivery nil/blank message */ }}
	for _, delivery := range deliveries {
		this.channelBuffer <- delivery
	}

	this.worker.Listen()

	this.So(this.handleCtx, should.Equal, this.hardContext)
	this.So(this.handleCount, should.Equal, 1)
	this.So(this.handleMessages, should.Resemble, []any{1, 3})

	this.So(this.acknowledgeContext, should.Equal, this.hardContext)
	this.So(this.acknowledgeCount, should.Equal, 1)
	this.So(this.acknowledgeDeliveries, should.Resemble, deliveries)
}
func (this *WorkerFixture) TestWhenMoreDeliveriesExistThanBatchMax_DeliverInBatchesOfMaxSpecifiedSize() {
	this.subscription.batchCapacity = 2
	this.subscription.bufferCapacity = 5
	this.initializeWorker()
	this.readError = io.EOF
	deliveries := []messaging.Delivery{{Message: 1}, {Message: 2}, {Message: 3}, {Message: 4}, {Message: 5}}
	for _, delivery := range deliveries {
		this.channelBuffer <- delivery
	}

	this.worker.Listen()

	this.So(this.handleCtx, should.Equal, this.hardContext)
	this.So(this.handleCount, should.Equal, 3)
	this.So(this.handleMessages, should.Resemble, []any{1, 2, 3, 4, 5})

	this.So(this.acknowledgeContext, should.Equal, this.hardContext)
	this.So(this.acknowledgeCount, should.Equal, 3)
	this.So(this.acknowledgeDeliveries, should.Resemble, deliveries)
}
func (this *WorkerFixture) TestWhenConfigured_PassFullDeliveryToHandler() {
	this.subscription.handleDelivery = true
	this.initializeWorker()
	this.readError = io.EOF
	this.channelBuffer <- messaging.Delivery{Message: 1}

	this.worker.Listen()

	this.So(this.handleCtx, should.Equal, this.hardContext)
	this.So(this.handleCount, should.Equal, 1)
	this.So(this.handleMessages, should.Resemble, []any{messaging.Delivery{Message: 1}})
}
func (this *WorkerFixture) TestWhenConfigured_PassFullDeliveryToContext() {
	this.subscription.deliveryToContext = true
	this.initializeWorker()
	this.readError = io.EOF
	this.channelBuffer <- messaging.Delivery{Message: 1}
	this.channelBuffer <- messaging.Delivery{Message: 2}

	this.worker.Listen()

	this.So(this.handleCtx.Value(ContextKeyDeliveries), should.Resemble, []messaging.Delivery{
		{Message: 1},
		{Message: 2},
	})
	this.So(this.handleCount, should.Equal, 1)
	this.So(this.handleMessages, should.Resemble, []any{1, 2})
}

func (this *WorkerFixture) TestWhenAcknowledgementFails_ListeningConcludesWithoutProcessingBufferedDeliveries() {
	this.acknowledgeError = errors.New("")
	this.readError = io.EOF
	this.subscription.batchCapacity = 1
	this.subscription.bufferCapacity = 2
	this.initializeWorker()
	this.channelBuffer <- messaging.Delivery{Message: 1}
	this.channelBuffer <- messaging.Delivery{Message: 2}

	this.worker.Listen()

	this.So(this.acknowledgeCount, should.Equal, 1)
	this.So(len(this.channelBuffer), should.Equal, 1)
}
func (this *WorkerFixture) SkipTestWhenConfiguredToBufferBetweenBatches_SleepAfterAcknowledgementAndNoMoreWork() {
	const timeout = time.Millisecond * 5
	this.readError = io.EOF
	this.subscription.bufferTimeout = timeout
	this.initializeWorker()

	this.channelBuffer <- messaging.Delivery{Message: 1}

	this.worker.Listen()

	this.So(time.Since(this.acknowledgeTimestamp[0]), should.BeBetween, timeout, timeout*3)
}
func (this *WorkerFixture) TestWhenConfiguredToBufferBetweenBatches_DoNotSleepAfterAcknowledgementIfMoreWorkAvailable() {
	const timeout = time.Millisecond * 5
	this.readError = io.EOF
	this.subscription.batchCapacity = 1
	this.subscription.bufferTimeout = timeout
	this.initializeWorker()

	this.channelBuffer <- messaging.Delivery{Message: 1} // first batch
	this.channelBuffer <- messaging.Delivery{Message: 1} // second bath

	this.worker.Listen()

	duration := this.acknowledgeTimestamp[0].Sub(this.acknowledgeTimestamp[1])
	this.So(duration, should.BeLessThan, timeout) // no sleep delay between first and second acknowledgements
	this.So(time.Since(this.acknowledgeTimestamp[1]), should.BeBetween, timeout, timeout*3)
	this.So(this.acknowledgeCount, should.Equal, 2)
}

func (this *WorkerFixture) TestWhenRequestingShutdownAndStrategyIsImmediate_DoNotDeliveryMoreToHandler() {
	this.readError = io.EOF
	this.subscription.shutdownStrategy = ShutdownStrategyImmediate
	this.initializeWorker()
	this.channelBuffer <- messaging.Delivery{Message: 1}

	this.softShutdown()
	this.worker.Listen()

	this.So(this.handleCount, should.Equal, 0)
}
func (this *WorkerFixture) TestWhenRequestingShutdownAndStrategyIsCurrentBatch_DeliverAndAckBatchThenExit() {
	this.readError = io.EOF
	this.subscription.batchCapacity = 2
	this.subscription.bufferTimeout = time.Second // should not be reached
	this.subscription.shutdownStrategy = ShutdownStrategyCurrentBatch
	this.initializeWorker()
	this.channelBuffer <- messaging.Delivery{Message: 1}
	this.channelBuffer <- messaging.Delivery{Message: 2}
	this.channelBuffer <- messaging.Delivery{Message: 3}

	this.softShutdown()
	this.worker.Listen()

	this.So(this.handleCount, should.Equal, 1)
	this.So(this.acknowledgeCount, should.Equal, 1)
	this.So(this.acknowledgeTimestamp[0], should.HappenWithin, time.Millisecond*25, time.Now().UTC()) // 1-second sleep is skipped
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
	this.acknowledgeTimestamp = append(this.acknowledgeTimestamp, time.Now().UTC())
	this.acknowledgeCount++
	this.acknowledgeContext = ctx
	this.acknowledgeDeliveries = append(this.acknowledgeDeliveries, deliveries...)
	return this.acknowledgeError
}
func (this *WorkerFixture) Close() error { panic("nop") }

func (this *WorkerFixture) Handle(ctx context.Context, messages ...any) {
	this.handleTimestamp = time.Now().UTC()
	this.handleCount++
	this.handleCtx = ctx
	this.handleMessages = append(this.handleMessages, messages...)
}
