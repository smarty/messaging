//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/smarty/gunit"
	"github.com/smarty/gunit/assert/should"
	"github.com/smarty/messaging/v4"
	"github.com/smarty/messaging/v4/rabbitmq"
	"github.com/smarty/messaging/v4/status"
)

func TestStatusFixture(t *testing.T) {
	gunit.Run(new(StatusFixture), t, gunit.Options.SequentialTestCases())
}

type StatusFixture struct {
	*gunit.Fixture

	ctx       context.Context
	shutdown  context.CancelFunc
	log       capturingLog
	transport messaging.Connector
}

func (this *StatusFixture) Setup() {
	this.ctx, this.shutdown = context.WithTimeout(context.Background(), time.Second*30)
	this.transport = rabbitmq.New(
		rabbitmq.Options.Address(brokerAddress),
		rabbitmq.Options.Logger(&this.log),
		rabbitmq.Options.BrokerTimeout(time.Second*5),
		// deliberately left at the default: a topology panic must not escape the probe
	)
}
func (this *StatusFixture) Teardown() {
	this.shutdown()
	_ = this.transport.Close()
}

func (this *StatusFixture) TestAgainstAHealthyBroker_TheProbePasses() {
	checker := status.New(status.Options.Connector(this.transport), status.Options.Logger(&this.log))
	ctx, cancel := context.WithTimeout(this.ctx, time.Second*5)
	defer cancel()

	this.So(checker.Status(ctx), should.BeNil)
	this.So(checker.Status(ctx), should.BeNil) // reuses the cached transactional writer
}

// A probe topic whose exchange does not exist is a configuration fault. It
// surfaces at commit inside the same probe, is definitive despite the
// tolerance window, and is returned rather than panicking the process.
func (this *StatusFixture) TestWhenTheProbeExchangeIsMissing_ReportAtOnceWithoutPanicking() {
	checker := status.New(
		status.Options.Connector(this.transport),
		status.Options.Topic(uniqueName("missing-probe-exchange")),
		status.Options.FailureTolerance(time.Minute),
		status.Options.Logger(&this.log),
	)
	ctx, cancel := context.WithTimeout(this.ctx, time.Second*5)
	defer cancel()

	var err error
	this.So(func() { err = checker.Status(ctx) }, should.NotPanic)

	var brokerErr *amqp.Error
	this.So(errors.As(err, &brokerErr), should.BeTrue)
	this.So(brokerErr.Code, should.Equal, amqp.NotFound)
	this.So(this.log.Contains("definitive"), should.BeTrue)
}
