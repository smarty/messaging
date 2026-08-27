package status

import (
	"context"
	"errors"
	"testing"

	"github.com/smarty/gunit"
	"github.com/smarty/gunit/assert/should"
	"github.com/smarty/messaging/v3"
)

func TestStatusFixture(t *testing.T) {
	gunit.Run(new(StatusFixture), t)
}

type StatusFixture struct {
	*gunit.Fixture

	ctx     context.Context
	checker Checker

	connectCalls int
	connectError error
	writerError  error
	writeCalls   int
	writeError   error
	closeCalls   int
}

func (this *StatusFixture) Setup() {
	this.ctx = context.Background()
	this.checker = New(Options.Connector(this), Options.Topic("status-topic"))
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
