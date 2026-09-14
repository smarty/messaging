//go:build integration

package integration

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/smarty/gunit"
	"github.com/smarty/gunit/assert/should"
	"github.com/smarty/messaging/v4"
	"github.com/smarty/messaging/v4/rabbitmq"
	"github.com/smarty/messaging/v4/status"
)

// This is a broker resource alarm as a timeline in three phases. Under a
// memory or disk alarm the broker blocks every connection that publishes: it
// sends connection.blocked and stops reading from the socket. A small publish
// lands in the kernel buffers and "succeeds"; the tx.commit behind it is never
// read, so the commit stalls. A large publish fills the buffers and stalls in
// the socket write itself. Connections that only consume are not blocked.
//
// The tests do not raise or clear the alarm. `make test.integration.alarm`
// runs them in order and sets node 1's memory watermark to zero between the
// first two phases and back to the default before the third. The queue name
// is shared through INTEGRATION_ALARM_QUEUE, and the tests skip when it is
// unset, so the ordinary `make test.integration` run never sees them.
//
// The consumer-cancel bound (Stream.Close) is not asserted here. A cancel
// frame parks only once the socket buffers are full, and any publish that
// fills them is itself bounded and severs the connection first, which frees
// the cancel. Its unit test covers the bound directly.
var alarmQueue = os.Getenv("INTEGRATION_ALARM_QUEUE")

const (
	alarmTimeout    = time.Second * 3
	alarmSettle     = time.Second * 30 // the management API reports the alarm on its stats interval
	largeBatchSize  = 64
	largeMessageLen = 1 << 20 // 1 MiB; the batch must exceed the socket buffers on both sides of the port forward
)

func TestResourceAlarmBefore(t *testing.T) { runAlarmPhase(t, new(ResourceAlarmBeforeFixture)) }
func TestResourceAlarmDuring(t *testing.T) { runAlarmPhase(t, new(ResourceAlarmDuringFixture)) }
func TestResourceAlarmAfter(t *testing.T)  { runAlarmPhase(t, new(ResourceAlarmAfterFixture)) }

func runAlarmPhase(t *testing.T, fixture any) {
	if alarmQueue == "" {
		t.Skip("INTEGRATION_ALARM_QUEUE is unset; the resource-alarm phases run through `make test.integration.alarm`")
	}
	gunit.Run(fixture, t, gunit.Options.SequentialTestCases())
}

// alarmPhase is the shared state of the three phase fixtures.
type alarmPhase struct {
	ctx        context.Context
	shutdown   context.CancelFunc
	management *management
	log        capturingLog
	monitor    rabbitMonitor
	exchange   string
	queue      string
	transport  messaging.Connector
}

func (this *alarmPhase) setup() {
	this.ctx, this.shutdown = context.WithTimeout(context.Background(), time.Minute*5)
	this.management = newManagement()
	this.queue = alarmQueue
	this.exchange = alarmQueue + "-exchange"
	this.transport = rabbitmq.New(
		rabbitmq.Options.Address(brokerAddress),
		rabbitmq.Options.Logger(&this.log),
		rabbitmq.Options.Monitor(&this.monitor),
		rabbitmq.Options.BrokerTimeout(alarmTimeout),
	)
}
func (this *alarmPhase) teardown() {
	_ = this.transport.Close()
	this.shutdown()
}
func (this *alarmPhase) publish(messageType string) error {
	return publish(this.ctx, this.transport, messaging.Dispatch{Topic: this.exchange, MessageType: messageType, Payload: []byte(messageType), Durable: true})
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Phase 1: no alarm. Declare the topology and prove a transactional publish
// commits and is delivered.
type ResourceAlarmBeforeFixture struct {
	*gunit.Fixture
	alarmPhase
}

func (this *ResourceAlarmBeforeFixture) Setup()    { this.setup() }
func (this *ResourceAlarmBeforeFixture) Teardown() { this.teardown() }

func (this *ResourceAlarmBeforeFixture) TestWithoutAlarm_ATransactionalPublishCommitsAndIsDelivered() {
	this.So(this.management.MemoryAlarm(), should.BeFalse)
	this.management.DeclareExchange(this.exchange)
	this.management.DeclareQueue(this.queue)
	this.management.Bind(this.queue, this.exchange)

	err := this.publish("before")

	this.So(err, should.BeNil)
	received, err := drain(this.ctx, this.transport, this.queue, time.Second*2)
	this.So(err, should.BeNil)
	this.So(received, should.Equal, []string{"before"})
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Phase 2: the memory alarm is on. Publishing connections are blocked and
// every wait on them returns within the bound, severs, and is reported.
type ResourceAlarmDuringFixture struct {
	*gunit.Fixture
	alarmPhase
}

func (this *ResourceAlarmDuringFixture) Setup() {
	this.setup()
	this.So(waitFor(alarmSettle, this.management.MemoryAlarm), should.BeTrue)
}
func (this *ResourceAlarmDuringFixture) Teardown() { this.teardown() }

// The small publish is buffered and the commit behind it is never read. The
// broker also announces the block, which reaches the log and the monitor.
func (this *ResourceAlarmDuringFixture) TestUnderAlarm_TheCommitIsBoundedSeveredAndTheBlockIsReported() {
	started := time.Now()
	err := this.publish("during")
	elapsed := time.Since(started)

	this.So(errors.Is(err, rabbitmq.ErrCommitTimeout), should.BeTrue)
	this.So(elapsed, should.BeGreaterThanOrEqualTo, alarmTimeout)
	this.So(elapsed, should.BeLessThan, alarmTimeout+time.Second*10) // the sever's close deadline bounds the rest
	this.So(this.log.Contains("[WARN] AMQP connection blocked by broker (reason:"), should.BeTrue)
	this.So(this.log.Contains("[WARN] AMQP transaction commit did not complete within [3s]; severing the connection."), should.BeTrue)
	this.So(this.monitor.Count("blocked"), should.BeGreaterThanOrEqualTo, 1)
	this.So(this.monitor.Count("commit-failed"), should.Equal, 1)
	this.So(this.monitor.Count("closed"), should.BeGreaterThanOrEqualTo, 1)
	// No unblock will ever arrive for a severed connection; the library reports
	// one itself so a blocked gauge does not stick at 1.
	this.So(waitFor(time.Second*5, func() bool { return this.monitor.Count("unblocked") >= 1 }), should.BeTrue)
}

// A batch too large for the buffers stalls in the socket write itself, before
// any commit. It times out with zero written so the caller retries all of it.
func (this *ResourceAlarmDuringFixture) TestUnderAlarm_ALargePublishIsBoundedAndReportsZeroWritten() {
	connection, err := this.transport.Connect(this.ctx)
	this.So(err, should.BeNil)
	defer func() { _ = connection.Close() }()
	writer, err := connection.Writer(this.ctx)
	this.So(err, should.BeNil)
	defer func() { _ = writer.Close() }()

	started := time.Now()
	count, err := writer.Write(this.ctx, largeBatch(this.exchange)...)
	elapsed := time.Since(started)

	this.So(errors.Is(err, rabbitmq.ErrPublishTimeout), should.BeTrue)
	this.So(count, should.Equal, 0)
	this.So(elapsed, should.BeGreaterThanOrEqualTo, alarmTimeout)
	this.So(elapsed, should.BeLessThan, alarmTimeout+time.Second*10)
	this.So(this.log.Contains("[WARN] AMQP publish did not complete within [3s]; severing the connection."), should.BeTrue)
}

// The status probe publishes and commits, so it fails within the bound instead
// of reporting a healthy broker that cannot accept a message.
func (this *ResourceAlarmDuringFixture) TestUnderAlarm_AStatusProbeFailsWithinTheBound() {
	checker := status.New(status.Options.Connector(this.transport), status.Options.FailureTolerance(0), status.Options.Logger(&this.log))
	ctx, cancel := context.WithTimeout(this.ctx, alarmTimeout+time.Second*15)
	defer cancel()

	err := checker.Status(ctx)

	this.So(errors.Is(err, rabbitmq.ErrCommitTimeout), should.BeTrue)
}

// A connection that only consumes is not blocked. Its stream reads, and its
// close returns at once rather than waiting on the broker.
func (this *ResourceAlarmDuringFixture) TestUnderAlarm_AConsumeOnlyConnectionIsUnaffected() {
	connection, err := this.transport.Connect(this.ctx)
	this.So(err, should.BeNil)
	reader, err := connection.Reader(this.ctx)
	this.So(err, should.BeNil)
	stream, err := reader.Stream(this.ctx, messaging.StreamConfig{StreamName: this.queue, BufferCapacity: 16})
	this.So(err, should.BeNil)

	readCtx, cancel := context.WithTimeout(this.ctx, time.Millisecond*500)
	defer cancel()
	readErr := stream.Read(readCtx, &messaging.Delivery{})
	started := time.Now()
	closeErr := errors.Join(stream.Close(), reader.Close(), connection.Close())
	elapsed := time.Since(started)

	this.So(readErr, should.Equal, context.DeadlineExceeded) // the queue is empty; nothing can be published into it
	this.So(closeErr, should.BeNil)
	this.So(elapsed, should.BeLessThan, alarmTimeout)
}

func largeBatch(topic string) (results []messaging.Dispatch) {
	payload := make([]byte, largeMessageLen)
	for i := 0; i < largeBatchSize; i++ {
		results = append(results, messaging.Dispatch{Topic: topic, MessageType: "large", Payload: payload})
	}
	return results
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Phase 3: the alarm is cleared. Publishing resumes with no restart, and what
// arrives is the lesson for callers.
type ResourceAlarmAfterFixture struct {
	*gunit.Fixture
	alarmPhase
}

func (this *ResourceAlarmAfterFixture) Setup() { this.setup() }
func (this *ResourceAlarmAfterFixture) Teardown() {
	this.management.DeleteQueue(this.queue) // the last phase cleans up
	this.management.DeleteExchange(this.exchange)
	this.teardown()
}

func (this *ResourceAlarmAfterFixture) TestAfterTheAlarmClears_PublishingResumesAndBufferedMessagesMayArrive() {
	this.So(waitFor(alarmSettle, func() bool { return !this.management.MemoryAlarm() }), should.BeTrue)

	err := this.publish("after")

	this.So(err, should.BeNil)
	received, err := drain(this.ctx, this.transport, this.queue, time.Second*2)
	this.So(err, should.BeNil)

	// The post-recovery message is there, and so may be anything the blocked
	// connections had already written into the socket: the broker reads the
	// buffered frames when the alarm clears, including a tx.commit that was
	// sent before the sever, and publishes what they carry. Both a commit
	// timeout and a publish timeout therefore mean "unknown", not "not
	// published". Consumers must be idempotent.
	this.So(received, should.NotBeEmpty)
	this.So(received[len(received)-1], should.Equal, "after")
	for _, messageType := range received[:len(received)-1] {
		this.So([]string{"during", "large", ""}, should.Contain, messageType) // the severed commit, the severed batch, and the probe's empty dispatch
	}
}
