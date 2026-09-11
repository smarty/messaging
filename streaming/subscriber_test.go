package streaming

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/smarty/gunit"
	"github.com/smarty/gunit/assert/should"
	"github.com/smarty/messaging/v4"
)

func TestSubscriberFixture(t *testing.T) {
	gunit.Run(new(SubscriberFixture), t)
}

type SubscriberFixture struct {
	*gunit.Fixture

	subscription Subscription
	softContext  context.Context
	softShutdown context.CancelFunc
	subscriber   messaging.Listener
	log          capturingLog
	monitor      capturingMonitor

	workerFactoryCount  int
	workerFactoryConfig workerConfig

	currentCount   int
	currentContext context.Context
	currentError   error

	releasedConnections []messaging.Connection

	readerCount int
	readerCtx   context.Context
	readerError error

	closeCount       int32
	connectionClosed bool

	streamCount   int
	streamContext context.Context
	streamConfig  messaging.StreamConfig
	streamError   error

	listenCount                int32
	softShutdownWhenListening  bool
	listenWaitForSoftShutdown  bool
	listenSleepForHardShutdown bool
	listenWaitForHardShutdown  bool
	listenSleep                time.Duration
}

func (this *SubscriberFixture) Setup() {
	this.subscription = Subscription{
		streamName:         "queue",
		subscriptionTopics: []string{"topic1", "topic2"},
		establishTopology:  true,
		bufferCapacity:     16,
		handlers:           []messaging.Handler{nil},
	}
	this.softContext, this.softShutdown = context.WithCancel(context.Background())
	this.initializeSubscriber()
}
func (this *SubscriberFixture) initializeSubscriber() {
	this.subscriber = newSubscriber(this, this.subscription, this.softContext, this.workerFactory, &this.log, &this.monitor)
}
func (this *SubscriberFixture) workerFactory(config workerConfig) messaging.Listener {
	this.workerFactoryCount++
	this.workerFactoryConfig = config
	return this
}

func (this *SubscriberFixture) TestWhenOpeningAConnectionFails_ListenShouldReturn() {
	this.currentError = errors.New("dial refused")

	this.subscriber.Listen()

	this.So(this.currentContext, should.Equal, this.softContext)
	this.So(this.currentCount, should.Equal, 1)
	this.So(this.log.String(), should.ContainSubstring, "[WARN] Unable to open connection for stream [queue] [dial refused].")
	this.So(this.monitor.Calls(), should.Equal, []string{"opened:queue:dial refused"})
}
func (this *SubscriberFixture) TestWhenOpeningReaderFails_ListenShouldReturn() {
	this.readerError = errors.New("channel refused")

	this.subscriber.Listen()

	this.So(this.readerCtx, should.Equal, this.softContext)
	this.So(this.readerCount, should.Equal, 1)
	this.So(this.releasedConnections, should.Equal, []messaging.Connection{this})
	this.So(this.log.String(), should.ContainSubstring, "[WARN] Unable to open reader for stream [queue] [channel refused].")
	this.So(this.monitor.Calls(), should.Equal, []string{"opened:queue:channel refused"})
}
func (this *SubscriberFixture) TestWhenOpeningStreamFails_ListenShouldReturn() {
	this.streamError = errors.New("NOT_FOUND - no exchange")

	this.subscriber.Listen()

	this.So(this.streamContext, should.Equal, this.softContext)
	this.So(this.streamCount, should.Equal, 1)
	this.So(this.streamConfig, should.Equal, messaging.StreamConfig{
		EstablishTopology: true,
		ExclusiveStream:   true, // single handler
		BufferCapacity:    this.subscription.bufferCapacity,
		StreamName:        this.subscription.streamName,
		Topics:            this.subscription.subscriptionTopics,
	})
	this.So(this.closeCount, should.Equal, 1)         // reader
	this.So(this.releasedConnections, should.BeEmpty) // channel-level failure: the shared connection stays up for siblings
	this.So(this.log.String(), should.ContainSubstring, "[WARN] Unable to open stream [queue] [NOT_FOUND - no exchange].")
	this.So(this.monitor.Calls(), should.Equal, []string{"opened:queue:NOT_FOUND - no exchange"})
}

func (this *SubscriberFixture) TestWhenListening_EstablishWorkersAndListen() {
	this.softShutdownWhenListening = true

	this.subscriber.Listen()

	this.So(this.workerFactoryCount, should.Equal, len(this.subscription.handlers))
	this.So(this.workerFactoryConfig.Now, should.NotBeNil)
	this.workerFactoryConfig.Now = nil // funcs never compare equal
	this.So(this.workerFactoryConfig, should.Equal, workerConfig{
		Stream:       this,
		Subscription: this.subscription,
		Handler:      nil,
		SoftContext:  this.softContext,
		HardContext:  this.subscriber.(defaultSubscriber).hardContext,
		Logger:       &this.log,
		Monitor:      &this.monitor,
	})
	this.So(this.listenCount, should.Equal, len(this.subscription.handlers))
}
func (this *SubscriberFixture) TestWhenListenConcludesOnShutdown_AllResourcesShouldBeClosed() {
	this.softShutdown()

	this.subscriber.Listen()

	this.So(this.closeCount, should.Equal, 2) // reader and stream
}
func (this *SubscriberFixture) TestWhenListeningConcludesWithoutShutdown_AllResourcesShouldBeClosed() {
	this.subscriber.Listen()

	this.So(this.closeCount, should.Equal, 2)         // reader and stream
	this.So(this.releasedConnections, should.BeEmpty) // the connection outlives one stream session
	this.So(this.log.String(), should.BeBlank)
	this.So(this.monitor.Calls(), should.Equal, []string{"opened:queue:<nil>", "closed:queue"})
}
func (this *SubscriberFixture) TestWhenConnectionIsClosedWhenTheSessionEnds_DisposeIt() {
	this.connectionClosed = true

	this.subscriber.Listen()

	this.So(this.releasedConnections, should.Equal, []messaging.Connection{this})
}
func (this *SubscriberFixture) TestWhenSoftShutdownIsInvoked_HardDeadlineShouldStart() {
	this.listenWaitForHardShutdown = true
	this.subscription.shutdownTimeout = time.Millisecond * 5
	this.initializeSubscriber()
	this.softShutdown()

	started := time.Now().UTC()
	this.subscriber.Listen()
	duration := time.Since(started)

	this.So(duration, should.BeGreaterThan, this.subscription.shutdownTimeout)
	_, hardDeadlineAlive := <-this.subscriber.(defaultSubscriber).hardContext.Done()
	this.So(hardDeadlineAlive, should.BeFalse)
	this.So(this.log.String(), should.ContainSubstring,
		"[WARN] Workers on stream [queue] did not conclude within [5ms] of shutdown; abandoning in-flight deliveries.")
	this.So(this.monitor.Calls(), should.Equal, []string{"opened:queue:<nil>", "forced:queue", "closed:queue"})
}
func (this *SubscriberFixture) SkipTestWhenSoftShutdownIsInvoked_ListenCanConcludeBeforeHardShutdownDeadline() {
	this.listenSleepForHardShutdown = true
	this.subscription.shutdownTimeout = time.Millisecond * 10
	this.initializeSubscriber()
	this.softShutdown()

	started := time.Now().UTC()
	this.subscriber.Listen()
	duration := time.Since(started)

	this.So(duration, should.BeGreaterThan, this.subscription.shutdownTimeout/2)
	this.So(duration, should.BeLessThan, this.subscription.shutdownTimeout)
}

func (this *SubscriberFixture) TestWhenShutdownStrategyIsImmediate_HardAndSoftShutdownContextsShouldBeTheSame() {
	this.subscription.shutdownStrategy = ShutdownStrategyImmediate
	this.subscription.shutdownTimeout = time.Millisecond * 5
	this.softShutdownWhenListening = true
	this.listenWaitForHardShutdown = true
	this.initializeSubscriber()

	started := time.Now().UTC()
	this.subscriber.Listen()

	this.So(time.Since(started), should.BeLessThan, this.subscription.shutdownTimeout*2)
}

func (this *SubscriberFixture) TestWhenHardShutdownIsInvoked_StillWaitForWorkersToConclude() {
	this.subscription.shutdownStrategy = ShutdownStrategyDrain
	this.subscription.shutdownTimeout = time.Millisecond * 3
	this.listenSleep = time.Millisecond * 7 // listeners take longer, even after hard shutdown
	this.softShutdownWhenListening = true
	this.listenWaitForHardShutdown = true
	this.initializeSubscriber()

	started := time.Now().UTC()
	this.subscriber.Listen()

	this.So(time.Since(started), should.BeGreaterThan, this.listenSleep)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// ConnectionPool
func (this *SubscriberFixture) Active(ctx context.Context) (messaging.Connection, error) {
	this.currentCount++
	this.currentContext = ctx
	return this, this.currentError
}
func (this *SubscriberFixture) Dispose(connection messaging.Connection) {
	this.releasedConnections = append(this.releasedConnections, connection)
}

// Connection
func (this *SubscriberFixture) Closed() bool { return this.connectionClosed }
func (this *SubscriberFixture) Reader(ctx context.Context) (messaging.Reader, error) {
	this.readerCount++
	this.readerCtx = ctx
	return this, this.readerError
}
func (this *SubscriberFixture) Writer(_ context.Context) (messaging.Writer, error) {
	panic("nop")
}
func (this *SubscriberFixture) CommitWriter(_ context.Context) (messaging.CommitWriter, error) {
	panic("nop")
}

// Reader
func (this *SubscriberFixture) Stream(ctx context.Context, config messaging.StreamConfig) (messaging.Stream, error) {
	this.streamCount++
	this.streamContext = ctx
	this.streamConfig = config
	return this, this.streamError
}

// Stream
func (this *SubscriberFixture) Read(_ context.Context, _ *messaging.Delivery) error {
	panic("nop")
}
func (this *SubscriberFixture) Acknowledge(_ context.Context, _ ...messaging.Delivery) error {
	panic("nop")
}

// Shared between Reader and Stream (connection isn't closed by the Subscriber)
func (this *SubscriberFixture) Close() error {
	atomic.AddInt32(&this.closeCount, 1)
	return nil
}

// Worker
func (this *SubscriberFixture) Listen() {
	atomic.AddInt32(&this.listenCount, 1)

	if this.softShutdownWhenListening {
		this.softShutdown()
	}

	if this.listenWaitForSoftShutdown {
		<-this.softContext.Done()
	}

	if this.listenSleep > 0 {
		time.Sleep(this.listenSleep)
	} else if this.listenSleepForHardShutdown {
		time.Sleep(this.subscription.shutdownTimeout / 2)
	} else if this.listenWaitForHardShutdown {
		<-this.subscriber.(defaultSubscriber).hardContext.Done()
	}
}
