package harness

import (
	"context"
	"testing"
	"time"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
)

func TestWaitFixture(t *testing.T) {
	gunit.Run(new(WaitFixture), t)
}

type WaitFixture struct {
	*gunit.Fixture
}

func (this *WaitFixture) TestContextAlreadyCancelled_ReturnImmediately() {
	ctx, cancel := context.WithCancel(this.Context())
	cancel()

	started := time.Now()
	err := wait(ctx, time.Hour)
	ended := time.Since(started)

	this.So(err, should.Equal, context.Canceled)
	this.So(ended, should.BeLessThan, time.Second)
}

func (this *WaitFixture) TestWait() {
	const duration = time.Millisecond * 5

	started := time.Now()
	err := wait(this.Context(), duration)
	ended := time.Since(started)

	this.So(err, should.BeNil)
	this.So(ended, should.BeGreaterThanOrEqualTo, duration)
}
