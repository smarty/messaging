package harness

import (
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
)

func TestTerminalFixture(t *testing.T) {
	gunit.Run(new(TerminalFixture), t)
}

type TerminalFixture struct {
	*gunit.Fixture
	input   chan *unitOfWork
	subject *terminal
}

func (this *TerminalFixture) Setup() {
	this.input = make(chan *unitOfWork, 4)
	this.subject = newTerminal(this.input)
}

func (this *TerminalFixture) TestDrainsInputUntilClosed() {
	this.input <- &unitOfWork{}
	this.input <- &unitOfWork{}
	this.input <- &unitOfWork{}
	close(this.input)

	done := make(chan struct{})
	go func() {
		this.subject.Listen()
		close(done)
	}()

	<-done
	this.So(len(this.input), should.Equal, 0)
}

func (this *TerminalFixture) TestEmptyClosedInputReturnsImmediately() {
	close(this.input)

	done := make(chan struct{})
	go func() {
		this.subject.Listen()
		close(done)
	}()

	<-done
}
