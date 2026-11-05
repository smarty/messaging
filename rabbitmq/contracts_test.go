package rabbitmq

import (
	"errors"
	"fmt"
	"testing"

	"github.com/smarty/gunit"
	"github.com/smarty/gunit/assert/should"
)

func TestContractsFixture(t *testing.T) {
	gunit.Run(new(ContractsFixture), t)
}

type ContractsFixture struct {
	*gunit.Fixture
}

func (this *ContractsFixture) TestBrokerTimeoutSentinels_ReportTimeoutEvenWhenWrapped() {
	for _, sentinel := range []error{ErrCommitTimeout, ErrPublishTimeout, ErrAcknowledgeTimeout, ErrCloseTimeout} {
		wrapped := fmt.Errorf("commit failed: %w", sentinel)

		this.So(isTimeout(wrapped), should.BeTrue)
		this.So(errors.Is(wrapped, sentinel), should.BeTrue)
	}
}
func (this *ContractsFixture) TestBrokerTimeoutSentinels_RemainDistinct() {
	this.So(ErrCommitTimeout, should.NotEqual, ErrPublishTimeout)
	this.So(errors.Is(ErrCommitTimeout, ErrAcknowledgeTimeout), should.BeFalse)
}
func (this *ContractsFixture) TestOtherSentinels_DoNotReportTimeout() {
	for _, sentinel := range []error{ErrAlreadyExclusive, ErrMultipleStreams, ErrInvalidHeader} {
		this.So(isTimeout(sentinel), should.BeFalse)
	}
}

// isTimeout is how a caller that does not import this package recognizes a timeout.
func isTimeout(err error) bool {
	var timeout interface{ Timeout() bool }
	return errors.As(err, &timeout) && timeout.Timeout()
}
