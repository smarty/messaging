//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/smarty/gunit"
	"github.com/smarty/gunit/assert/should"
	"github.com/smarty/messaging/v4"
	"github.com/smarty/messaging/v4/rabbitmq"
)

func TestBrokerEventsFixture(t *testing.T) {
	gunit.Run(new(BrokerEventsFixture), t, gunit.Options.SequentialTestCases())
}

type BrokerEventsFixture struct {
	*gunit.Fixture

	ctx        context.Context
	shutdown   context.CancelFunc
	management *management
	log        capturingLog
	monitor    rabbitMonitor
	transport  messaging.Connector
}

func (this *BrokerEventsFixture) Setup() {
	this.ctx, this.shutdown = context.WithTimeout(context.Background(), time.Second*60)
	this.management = newManagement()
	this.transport = rabbitmq.New(
		rabbitmq.Options.Address(brokerAddress),
		rabbitmq.Options.Logger(&this.log),
		rabbitmq.Options.Monitor(&this.monitor),
		rabbitmq.Options.BrokerTimeout(time.Second*5),
	)
}
func (this *BrokerEventsFixture) Teardown() {
	this.shutdown()
	_ = this.transport.Close()
}

// A close the broker initiates (an operator, a node shutdown, a policy) must
// reach the monitor and the log, and the connection must know it is dead.
func (this *BrokerEventsFixture) TestWhenTheBrokerForcesAConnectionClosed_TheLibraryReportsIt() {
	connection, err := this.transport.Connect(this.ctx)
	this.So(err, should.BeNil)
	defer func() { _ = connection.Close() }()
	reporter, ok := connection.(interface{ Closed() bool })
	this.So(ok, should.BeTrue)
	this.So(reporter.Closed(), should.BeFalse)

	// the management plugin learns of connections on its stats interval
	var names []string
	this.So(waitFor(time.Second*20, func() bool { names = this.management.Connections(); return len(names) > 0 }), should.BeTrue)
	for _, name := range names {
		this.management.CloseConnection(name)
	}

	this.So(waitFor(time.Second*10, func() bool { return this.monitor.Count("closed") >= 1 }), should.BeTrue)
	this.So(reporter.Closed(), should.BeTrue)
	this.So(this.log.Contains("[WARN] AMQP connection closed by the broker or network ["), should.BeTrue)
	this.So(this.log.Contains("CONNECTION_FORCED"), should.BeTrue)

	_ = connection.Close() // the owner's later close must not report a second time
	this.So(this.monitor.Count("closed"), should.Equal, 1)
}
