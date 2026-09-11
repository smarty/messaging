package sqlmq

import (
	"context"
	"testing"
	"time"

	"github.com/smarty/gunit"
	"github.com/smarty/gunit/assert/should"
	"github.com/smarty/messaging/v4"
)

func TestDeferredHandoffsFixture(t *testing.T) {
	gunit.Run(new(DeferredHandoffsFixture), t)
}

type DeferredHandoffsFixture struct {
	*gunit.Fixture

	ctx      context.Context
	shutdown context.CancelFunc
	channel  chan messaging.Dispatch
	deferred *deferredHandoffs
}

func (this *DeferredHandoffsFixture) Setup() {
	this.ctx, this.shutdown = context.WithCancel(context.Background())
	this.channel = make(chan messaging.Dispatch, 1)
	this.deferred = newDeferredHandoffs(this.channel, 4)
}
func (this *DeferredHandoffsFixture) Teardown() {
	this.shutdown()
}

func (this *DeferredHandoffsFixture) TestWhenUnderCapacity_DeferAndDeliverOnceChannelHasRoom() {
	this.channel <- messaging.Dispatch{} // full
	dispatches := []messaging.Dispatch{{MessageID: 1}, {MessageID: 2}}

	accepted := this.deferred.TryDefer(this.ctx, dispatches)

	this.So(accepted, should.BeTrue)
	this.So(this.deferred.pending.Load(), should.Equal, 2)
	this.So(receiveDispatch(this.channel), should.Equal, messaging.Dispatch{})
	this.So(receiveDispatch(this.channel), should.Equal, messaging.Dispatch{MessageID: 1})
	this.So(receiveDispatch(this.channel), should.Equal, messaging.Dispatch{MessageID: 2})
}
func (this *DeferredHandoffsFixture) TestWhenDeferralWouldExceedCapacity_RefuseAndLeavePendingUnchanged() {
	this.channel <- messaging.Dispatch{} // full
	this.So(this.deferred.TryDefer(this.ctx, []messaging.Dispatch{{}, {}, {}}), should.BeTrue)

	accepted := this.deferred.TryDefer(this.ctx, []messaging.Dispatch{{}, {}})

	this.So(accepted, should.BeFalse)
	this.So(this.deferred.pending.Load(), should.Equal, 3)
}
func (this *DeferredHandoffsFixture) TestWhenContextEnds_GoroutineExitsAndPendingReturnsToZero() {
	this.channel <- messaging.Dispatch{} // full
	this.So(this.deferred.TryDefer(this.ctx, []messaging.Dispatch{{}, {}}), should.BeTrue)

	this.shutdown()

	this.So(eventually(func() bool { return this.deferred.pending.Load() == 0 }), should.BeTrue)
	this.So(len(this.channel), should.Equal, 1) // nothing more was delivered
}
func (this *DeferredHandoffsFixture) TestWhenFullyDelivered_PendingReturnsToZero() {
	this.So(this.deferred.TryDefer(this.ctx, []messaging.Dispatch{{MessageID: 1}, {MessageID: 2}}), should.BeTrue)

	this.So(receiveDispatch(this.channel), should.Equal, messaging.Dispatch{MessageID: 1})
	this.So(receiveDispatch(this.channel), should.Equal, messaging.Dispatch{MessageID: 2})
	this.So(eventually(func() bool { return this.deferred.pending.Load() == 0 }), should.BeTrue)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func receiveDispatch(channel chan messaging.Dispatch) messaging.Dispatch {
	select {
	case dispatch := <-channel:
		return dispatch
	case <-time.After(time.Millisecond * 100):
		return messaging.Dispatch{MessageID: ^uint64(0)}
	}
}
func eventually(condition func() bool) bool {
	deadline := time.Now().Add(time.Millisecond * 100)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return condition()
}
