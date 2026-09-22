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

// This is the September 2026 incident, reproduced on purpose, as a timeline
// in three phases. A quorum queue with two of its three nodes stopped is in
// minority: it has no leader, so a publish routed to it is accepted by the
// channel and never confirmed, and tx.commit never returns. Before v4.1.0
// that wait had no bound.
//
// The tests do not stop or start anything. `make test.integration.ghost`
// runs them in order and stops and starts cluster nodes between them; each
// test only checks that the cluster is in the state its phase expects. The
// queue name is shared across the three processes through
// INTEGRATION_GHOST_QUEUE, and the tests skip when it is unset, so the
// ordinary `make test.integration` run never sees them.
var ghostQueue = os.Getenv("INTEGRATION_GHOST_QUEUE")

const (
	ghostTimeout   = time.Second * 3
	clusterSize    = 3
	recoveryBudget = time.Second * 180 // a restarting node may wait on peers in 30-second retry cycles
)

func TestGhostedQueueBefore(t *testing.T) { runGhostPhase(t, new(GhostedQueueBeforeFixture)) }
func TestGhostedQueueDuring(t *testing.T) { runGhostPhase(t, new(GhostedQueueDuringFixture)) }
func TestGhostedQueueAfter(t *testing.T)  { runGhostPhase(t, new(GhostedQueueAfterFixture)) }

func runGhostPhase(t *testing.T, fixture any) {
	if ghostQueue == "" {
		t.Skip("INTEGRATION_GHOST_QUEUE is unset; the ghosted-queue phases run through `make test.integration.ghost`")
	}
	gunit.Run(fixture, t, gunit.Options.SequentialTestCases())
}

// ghostPhase is the shared state of the three phase fixtures.
type ghostPhase struct {
	ctx        context.Context
	shutdown   context.CancelFunc
	management *management
	log        capturingLog
	monitor    rabbitMonitor
	exchange   string
	queue      string
	transport  messaging.Connector
}

func (this *ghostPhase) setup() {
	this.ctx, this.shutdown = context.WithTimeout(context.Background(), time.Minute*5)
	this.management = newManagement()
	this.queue = ghostQueue
	this.exchange = ghostQueue + "-exchange"
	this.transport = rabbitmq.New(
		rabbitmq.Options.Address(brokerAddress),
		rabbitmq.Options.Logger(&this.log),
		rabbitmq.Options.Monitor(&this.monitor),
		rabbitmq.Options.BrokerTimeout(ghostTimeout),
	)
}
func (this *ghostPhase) teardown() {
	_ = this.transport.Close()
	this.shutdown()
}
func (this *ghostPhase) publish(messageType string) error {
	return publish(this.ctx, this.transport, messaging.Dispatch{Topic: this.exchange, MessageType: messageType, Payload: []byte(messageType), Durable: true})
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Phase 1: the cluster is whole. Declare the topology and prove a transactional
// publish commits and is delivered.
type GhostedQueueBeforeFixture struct {
	*gunit.Fixture
	ghostPhase
}

func (this *GhostedQueueBeforeFixture) Setup()    { this.setup() }
func (this *GhostedQueueBeforeFixture) Teardown() { this.teardown() }

func (this *GhostedQueueBeforeFixture) TestWithQuorum_ATransactionalPublishCommitsAndIsDelivered() {
	this.So(this.management.RunningNodes(), should.Equal, clusterSize)
	this.management.DeclareExchange(this.exchange)
	this.management.DeclareQuorumQueue(this.queue)
	this.management.Bind(this.queue, this.exchange)
	this.So(waitFor(time.Second*30, func() bool { return this.management.QuorumOnline(this.queue) == clusterSize }), should.BeTrue)

	err := this.publish("before")

	this.So(err, should.BeNil)
	received, err := drain(this.ctx, this.transport, this.queue, time.Second*2)
	this.So(err, should.BeNil)
	this.So(received, should.Equal, []string{"before"})
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Phase 2: two nodes are stopped and the queue is in minority. The commit that
// stalled for hours in production returns within the bound, severs the
// connection, and is visible to logs, monitors, and the status probe.
type GhostedQueueDuringFixture struct {
	*gunit.Fixture
	ghostPhase
}

func (this *GhostedQueueDuringFixture) Setup()    { this.setup() }
func (this *GhostedQueueDuringFixture) Teardown() { this.teardown() }

func (this *GhostedQueueDuringFixture) TestInMinority_CommitIsBoundedSeveredAndReported() {
	this.So(waitFor(time.Second*60, func() bool { return this.management.RunningNodes() == 1 }), should.BeTrue)
	this.So(waitFor(time.Second*30, func() bool { return this.management.QuorumOnline(this.queue) == 1 }), should.BeTrue)

	started := time.Now()
	err := this.publish("during")
	elapsed := time.Since(started)

	this.So(errors.Is(err, rabbitmq.ErrCommitTimeout), should.BeTrue)
	this.So(elapsed, should.BeGreaterThanOrEqualTo, ghostTimeout)
	this.So(elapsed, should.BeLessThan, ghostTimeout+time.Second*10) // the sever's close deadline bounds the rest
	this.So(this.log.Contains("[WARN] AMQP transaction commit did not complete within [3s]; severing the connection."), should.BeTrue)
	this.So(this.monitor.Count("commit-failed"), should.Equal, 1)
	this.So(this.monitor.Count("closed"), should.BeGreaterThanOrEqualTo, 1)
}

// A status probe aimed at the affected exchange fails within the bound instead
// of reporting healthy. The default probe topic would not see this, which the
// README says.
func (this *GhostedQueueDuringFixture) TestInMinority_AStatusProbeOnTheExchangeFailsWithinTheBound() {
	this.So(waitFor(time.Second*60, func() bool { return this.management.RunningNodes() == 1 }), should.BeTrue)
	checker := status.New(status.Options.Connector(this.transport), status.Options.Topic(this.exchange), status.Options.FailureTolerance(0), status.Options.Logger(&this.log))
	ctx, cancel := context.WithTimeout(this.ctx, ghostTimeout+time.Second*15)
	defer cancel()

	err := checker.Status(ctx)

	this.So(errors.Is(err, rabbitmq.ErrCommitTimeout), should.BeTrue)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Phase 3: the nodes are back. The queue regains quorum, publishing resumes with
// no restart, and what arrives is the lesson for callers.
type GhostedQueueAfterFixture struct {
	*gunit.Fixture
	ghostPhase
}

func (this *GhostedQueueAfterFixture) Setup() { this.setup() }
func (this *GhostedQueueAfterFixture) Teardown() {
	this.management.DeleteQueue(this.queue) // the last phase cleans up
	this.management.DeleteExchange(this.exchange)
	this.teardown()
}

func (this *GhostedQueueAfterFixture) TestAfterRecovery_PublishingResumesAndTheSeveredMessageMayArrive() {
	if !waitFor(recoveryBudget, func() bool { return this.management.RunningNodes() == clusterSize }) {
		this.Printf("cluster did not recover: nodes=%v", this.management.Nodes())
		this.So(false, should.BeTrue)
		return
	}
	if !waitFor(recoveryBudget, func() bool { return this.management.QuorumOnline(this.queue) == clusterSize }) {
		this.Printf("quorum queue did not regain all members: %s", this.management.QuorumStatus(this.queue))
		this.So(false, should.BeTrue)
		return
	}

	err := this.publish("after")

	this.So(err, should.BeNil)
	received, err := drain(this.ctx, this.transport, this.queue, time.Second*2)
	this.So(err, should.BeNil)

	// The post-recovery message is there, and so may be the messages from the
	// severed transaction and the failed probe: the channel had already handed
	// them to the queue's log, and the queue committed them once quorum
	// returned. A commit timeout therefore means "unknown", not "not
	// published". The outbox retries such a batch, so consumers see it twice.
	// Idempotency is not optional.
	this.So(received, should.NotBeEmpty)
	this.So(received[len(received)-1], should.Equal, "after")
	for _, messageType := range received[:len(received)-1] {
		this.So([]string{"during", ""}, should.Contain, messageType) // the severed publish, and the probe's empty dispatch
	}
}
