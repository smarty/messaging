package rabbitmq

import (
	"errors"
	"testing"
	"time"

	"github.com/smarty/gunit"
	"github.com/smarty/gunit/assert/should"
)

func TestAwaitBrokerFixture(t *testing.T) {
	gunit.Run(new(AwaitBrokerFixture), t)
}

type AwaitBrokerFixture struct {
	*gunit.Fixture

	logger     *capturingLogger
	released   chan struct{} // closed by sever; a well-behaved adapter errors the pending call
	severCalls int
}

func (this *AwaitBrokerFixture) Setup() {
	this.logger = &capturingLogger{lines: make(chan string, 4)}
	this.released = make(chan struct{})
}
func (this *AwaitBrokerFixture) await(call func() error) error {
	return awaitBroker(this.logger, "transaction commit", time.Millisecond*2, this.sever, ErrCommitTimeout, call)
}
func (this *AwaitBrokerFixture) sever() error {
	this.severCalls++
	close(this.released)
	return nil
}

func (this *AwaitBrokerFixture) TestWhenTheCallCompletesInTime_ReturnItsResult() {
	expected := errors.New("broker said no")

	err := this.await(func() error { return expected })

	this.So(err, should.Equal, expected)
	this.So(this.severCalls, should.Equal, 0)
	this.So(receive(this.logger.lines), should.Equal, "")
}
func (this *AwaitBrokerFixture) TestWhenTheCallExceedsTheTimeout_SeverAndReturnTheSentinel() {
	err := this.await(func() error { <-this.released; return errors.New("connection closed") })

	this.So(err, should.Equal, ErrCommitTimeout)
	this.So(this.severCalls, should.Equal, 1)
	this.So(receive(this.logger.lines), should.ContainSubstring, "severing the connection")
	this.So(receive(this.logger.lines), should.Equal, "")
}
func (this *AwaitBrokerFixture) TestWhenTheSeveredCallStillNeverReturns_GiveUpAfterASecondTimeout() {
	never := make(chan struct{})
	defer close(never)

	err := this.await(func() error { <-never; return nil }) // an adapter that breaks the invariant; without a second bound this parks forever

	this.So(err, should.Equal, ErrCommitTimeout)
	this.So(this.severCalls, should.Equal, 1)
	this.So(receive(this.logger.lines), should.ContainSubstring, "severing the connection")
	this.So(receive(this.logger.lines), should.ContainSubstring, "abandoning")
}
