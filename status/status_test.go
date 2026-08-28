package status

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/smarty/gunit"
	"github.com/smarty/gunit/assert/should"
	"github.com/smarty/messaging/v4"
)

func TestStatusFixture(t *testing.T) {
	gunit.Run(new(StatusFixture), t)
}

type StatusFixture struct {
	*gunit.Fixture

	ctx     context.Context
	checker Checker
	clock   time.Time

	connectCalls int
	connectError error
	writerError  error
	writeCalls   int
	writeError   error
	closeCalls   int
}

func (this *StatusFixture) Setup() {
	this.ctx = context.Background()
	this.clock = time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	this.checker = this.newChecker(Options.FailureTolerance(0))
}
func (this *StatusFixture) newChecker(options ...option) Checker {
	return New(append([]option{
		Options.Connector(this),
		Options.Topic("status-topic"),
		Options.Now(func() time.Time { return this.clock }),
	}, options...)...)
}
func (this *StatusFixture) advance(interval time.Duration) {
	this.clock = this.clock.Add(interval)
}

func (this *StatusFixture) TestWhenConnectFails_ReturnUnderlyingError() {
	this.connectError = errors.New("connect failed")

	err := this.checker.Status(this.ctx)

	this.So(err, should.Equal, this.connectError)
}

func (this *StatusFixture) TestWhenWriteFails_ReturnUnderlyingErrorAndDiscardCachedConnection() {
	this.writeError = errors.New("write failed")

	err := this.checker.Status(this.ctx)

	this.So(err, should.Equal, this.writeError)
	this.So(this.closeCalls, should.Equal, 2) // connection and writer both closed

	this.writeError = nil
	_ = this.checker.Status(this.ctx)
	this.So(this.connectCalls, should.Equal, 2) // next probe dials fresh
}

func (this *StatusFixture) TestWhenWriteFailsWithPasswordError_ReturnUnderlyingError() {
	this.writeError = errors.New("ACCESS_REFUSED: bad password")

	err := this.checker.Status(this.ctx)

	this.So(err, should.Equal, this.writeError)
}

func (this *StatusFixture) TestWhenWriteSucceeds_ReturnNilAndReuseCachedConnection() {
	firstError := this.checker.Status(this.ctx)
	secondError := this.checker.Status(this.ctx)

	this.So(firstError, should.BeNil)
	this.So(secondError, should.BeNil)
	this.So(this.connectCalls, should.Equal, 1)
	this.So(this.writeCalls, should.Equal, 2)
	this.So(this.closeCalls, should.Equal, 0)
}

func (this *StatusFixture) TestFailureTolerance_ToleratesFailuresWithinTheWindow() {
	this.checker = this.newChecker(Options.FailureTolerance(time.Second * 30))
	this.writeError = errors.New("write failed")

	first := this.checker.Status(this.ctx)
	this.advance(time.Second * 10)
	second := this.checker.Status(this.ctx)
	this.advance(time.Second * 19)
	third := this.checker.Status(this.ctx)
	this.advance(time.Second * 1)
	fourth := this.checker.Status(this.ctx)

	this.So(first, should.BeNil)                   // window opens
	this.So(second, should.BeNil)                  // 10s elapsed
	this.So(third, should.BeNil)                   // 29s elapsed
	this.So(fourth, should.Equal, this.writeError) // 30s elapsed
}
func (this *StatusFixture) TestFailureTolerance_SuccessResetsTheWindow() {
	this.checker = this.newChecker(Options.FailureTolerance(time.Second * 30))

	this.writeError = errors.New("write failed")
	_ = this.checker.Status(this.ctx) // window opens
	this.advance(time.Second * 10)
	this.writeError = nil
	success := this.checker.Status(this.ctx) // window resets
	this.advance(time.Second * 5)
	this.writeError = errors.New("write failed")
	reopened := this.checker.Status(this.ctx) // new window opens
	this.advance(time.Second * 25)
	within := this.checker.Status(this.ctx) // 25s into the new window (40s since the first)
	this.advance(time.Second * 5)
	expired := this.checker.Status(this.ctx) // 30s into the new window

	this.So(success, should.BeNil)
	this.So(reopened, should.BeNil)
	this.So(within, should.BeNil)
	this.So(expired, should.Equal, this.writeError)
}
func (this *StatusFixture) TestFailureTolerance_DefaultIsThirtySeconds() {
	this.checker = New(
		Options.Connector(this),
		Options.Now(func() time.Time { return this.clock }),
	)
	this.writeError = errors.New("write failed")

	first := this.checker.Status(this.ctx)
	this.advance(time.Second * 29)
	second := this.checker.Status(this.ctx)
	this.advance(time.Second * 1)
	third := this.checker.Status(this.ctx)

	this.So(first, should.BeNil)
	this.So(second, should.BeNil)
	this.So(third, should.Equal, this.writeError)
}
func (this *StatusFixture) TestWhenAccessIsRefused_FailFastDespiteTolerance() {
	this.checker = this.newChecker(Options.FailureTolerance(time.Second * 30))
	this.writeError = amqp.ErrCredentials

	err := this.checker.Status(this.ctx)

	this.So(err, should.Equal, this.writeError)
}
func (this *StatusFixture) TestWhenOperationNotAllowed_FailFastDespiteTolerance_EvenWhenWrapped() {
	this.checker = this.newChecker(Options.FailureTolerance(time.Second * 30))
	this.writeError = fmt.Errorf("status probe: %w", &amqp.Error{Code: amqp.NotAllowed, Reason: "vhost not found"})

	err := this.checker.Status(this.ctx)

	this.So(err, should.Equal, this.writeError)
}
func (this *StatusFixture) TestDefinitiveError_DoesNotOpenTheToleranceWindow() {
	this.checker = this.newChecker(Options.FailureTolerance(time.Second * 30))

	this.writeError = amqp.ErrCredentials
	_ = this.checker.Status(this.ctx)
	this.advance(time.Minute)
	this.writeError = errors.New("transient")

	err := this.checker.Status(this.ctx)

	this.So(err, should.BeNil) // the transient failure opens its own fresh window
}
func (this *StatusFixture) TestFailureToleranceOfZero_ReturnsTheFirstError() {
	this.checker = this.newChecker(Options.FailureTolerance(0))
	this.writeError = errors.New("write failed")

	err := this.checker.Status(this.ctx)

	this.So(err, should.Equal, this.writeError)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *StatusFixture) Connect(_ context.Context) (messaging.Connection, error) {
	this.connectCalls++
	if this.connectError != nil {
		return nil, this.connectError
	}
	return this, nil
}
func (this *StatusFixture) Writer(_ context.Context) (messaging.Writer, error) {
	if this.writerError != nil {
		return nil, this.writerError
	}
	return this, nil
}
func (this *StatusFixture) Write(_ context.Context, dispatches ...messaging.Dispatch) (int, error) {
	this.writeCalls++
	if this.writeError != nil {
		return 0, this.writeError
	}
	return len(dispatches), nil
}
func (this *StatusFixture) Close() error { this.closeCalls++; return nil }

func (this *StatusFixture) Reader(_ context.Context) (messaging.Reader, error) { panic("nop") }
func (this *StatusFixture) CommitWriter(_ context.Context) (messaging.CommitWriter, error) {
	panic("nop")
}
