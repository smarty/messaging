//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/smarty/gunit"
	"github.com/smarty/gunit/assert/should"
	"github.com/smarty/messaging/v4"
	"github.com/smarty/messaging/v4/rabbitmq"
	"github.com/smarty/messaging/v4/status"
)

// This is the September 2026 incident, reproduced on purpose. A quorum queue
// with two of its three nodes stopped is in minority: it has no leader, so a
// publish routed to it is accepted by the channel and never confirmed, and
// tx.commit never returns. Before v4.1.0 that wait had no bound.
func TestGhostedQueueFixture(t *testing.T) {
	if !composeAvailable() {
		t.Skipf("no compose file at %s; set INTEGRATION_COMPOSE_FILE to run cluster tests", composeFile)
	}
	gunit.Run(new(GhostedQueueFixture), t, gunit.Options.SequentialTestCases())
}

const (
	ghostTimeout = time.Second * 3
	clusterSize  = 3
	// A restarting node may wait on peers in 30-second retry cycles, so
	// recovery gets a generous budget.
	recoveryBudget = time.Second * 180
)

type GhostedQueueFixture struct {
	*gunit.Fixture

	ctx        context.Context
	shutdown   context.CancelFunc
	management *management
	log        capturingLog
	monitor    rabbitMonitor
	exchange   string
	queue      string
	transport  messaging.Connector
}

func (this *GhostedQueueFixture) Setup() {
	this.ctx, this.shutdown = context.WithTimeout(context.Background(), time.Minute*5)
	this.management = newManagement()
	this.exchange = uniqueName("ghost-exchange")
	this.queue = uniqueName("ghost-queue")
	this.transport = rabbitmq.New(
		rabbitmq.Options.Address(brokerAddress),
		rabbitmq.Options.Logger(&this.log),
		rabbitmq.Options.Monitor(&this.monitor),
		rabbitmq.Options.BrokerTimeout(ghostTimeout),
	)
	this.So(this.awaitCluster(), should.BeTrue) // all nodes running before we start
	this.management.DeclareExchange(this.exchange)
	this.management.DeclareQuorumQueue(this.queue)
	this.management.Bind(this.queue, this.exchange)
	this.So(waitFor(time.Second*30, func() bool { return this.management.QuorumLeader(this.queue) != "" }), should.BeTrue)
}
func (this *GhostedQueueFixture) Teardown() {
	this.restoreCluster() // whatever happened, leave the cluster whole
	this.management.DeleteQueue(this.queue)
	this.management.DeleteExchange(this.exchange)
	_ = this.transport.Close()
	this.shutdown()
}
func (this *GhostedQueueFixture) awaitCluster() bool {
	if waitFor(recoveryBudget, func() bool { return this.management.RunningNodes() == clusterSize }) {
		return true
	}
	this.Printf("cluster did not recover: nodes=%v", this.management.Nodes())
	logs, _ := composeOutput("logs", "--tail", "60", "rabbitmq2", "rabbitmq3")
	this.Printf("%s", logs)
	return false
}

// ghostCluster stops two of three nodes, one after the other, so that a
// quorum queue with three members is left in minority.
func (this *GhostedQueueFixture) ghostCluster() bool {
	return compose("stop", "rabbitmq2") == nil && compose("stop", "rabbitmq3") == nil &&
		waitFor(time.Second*60, func() bool { return this.management.RunningNodes() == 1 })
}

// restoreCluster starts the nodes in the reverse order they stopped. RabbitMQ
// expects the last node down to be the first node up; starting both at once
// can leave each waiting on the other through several retry cycles.
func (this *GhostedQueueFixture) restoreCluster() bool {
	if err := compose("start", "rabbitmq3"); err != nil {
		return false
	}
	waitFor(time.Second*90, func() bool { return this.management.RunningNodes() >= 2 })
	if err := compose("start", "rabbitmq2"); err != nil {
		return false
	}
	return this.awaitCluster()
}

func (this *GhostedQueueFixture) TestQuorumQueueInMinority_CommitIsBoundedSeveredReportedAndRecovers() {
	// 1. With quorum, a transactional publish commits and is delivered.
	this.So(publish(this.ctx, this.transport, messaging.Dispatch{Topic: this.exchange, MessageType: "before", Payload: []byte("x"), Durable: true}), should.BeNil)
	received, err := drain(this.ctx, this.transport, this.queue, time.Second*2)
	this.So(err, should.BeNil)
	this.So(received, should.Equal, []string{"before"})

	// 2. Ghost the queue: stop two of three nodes. The queue keeps its record
	//    but loses its leader.
	this.So(this.ghostCluster(), should.BeTrue)
	this.So(waitFor(time.Second*30, func() bool { return this.management.QuorumOnline(this.queue) == 1 }), should.BeTrue) // minority

	// 3. The commit that stalled for hours in production now returns within
	//    the bound, severs the connection, and is visible to logs and monitors.
	started := time.Now()
	err = publish(this.ctx, this.transport, messaging.Dispatch{Topic: this.exchange, MessageType: "during", Payload: []byte("x"), Durable: true})
	elapsed := time.Since(started)

	this.So(errors.Is(err, rabbitmq.ErrCommitTimeout), should.BeTrue)
	this.So(elapsed, should.BeGreaterThanOrEqualTo, ghostTimeout)
	this.So(elapsed, should.BeLessThan, ghostTimeout+time.Second*10) // the sever's close deadline bounds the rest
	this.So(this.log.Contains("[WARN] AMQP transaction commit did not complete within [3s]; severing the connection."), should.BeTrue)
	this.So(this.monitor.Count("commit-failed"), should.Equal, 1)
	this.So(this.monitor.Count("closed"), should.BeGreaterThanOrEqualTo, 1)

	// 4. A status probe aimed at the affected exchange fails within the bound
	//    instead of reporting healthy. (The default probe topic would not see
	//    this, which the README says.)
	checker := status.New(status.Options.Connector(this.transport), status.Options.Topic(this.exchange), status.Options.FailureTolerance(0), status.Options.Logger(&this.log))
	probeCtx, cancel := context.WithTimeout(this.ctx, ghostTimeout+time.Second*15)
	defer cancel()
	this.So(errors.Is(checker.Status(probeCtx), rabbitmq.ErrCommitTimeout), should.BeTrue)

	// 5. Bring the nodes back. The queue regains quorum and the publisher
	//    resumes with no restart.
	this.So(this.restoreCluster(), should.BeTrue)
	this.So(waitFor(time.Second*60, func() bool { return this.management.QuorumOnline(this.queue) == clusterSize }), should.BeTrue)
	this.So(publish(this.ctx, this.transport, messaging.Dispatch{Topic: this.exchange, MessageType: "after", Payload: []byte("x"), Durable: true}), should.BeNil)
	received, err = drain(this.ctx, this.transport, this.queue, time.Second*2)
	this.So(err, should.BeNil)

	// 6. What arrives is the lesson for callers. The post-recovery message is
	//    there, and so may be the messages from the severed transaction and
	//    the failed probe: the channel had already handed them to the queue's
	//    log, and the queue committed them once quorum returned. A commit
	//    timeout therefore means "unknown", not "not published". The outbox
	//    retries such a batch, so consumers see it twice. Idempotency is not
	//    optional.
	this.So(received, should.NotBeEmpty)
	this.So(received[len(received)-1], should.Equal, "after")
	for _, messageType := range received[:len(received)-1] {
		this.So([]string{"during", ""}, should.Contain, messageType) // the severed publish, and the probe's empty dispatch
	}
}
