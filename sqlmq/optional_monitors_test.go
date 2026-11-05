package sqlmq

import (
	"testing"

	"github.com/smarty/gunit"
	"github.com/smarty/gunit/assert/should"
)

func TestOptionalMonitorsFixture(t *testing.T) {
	gunit.Run(new(OptionalMonitorsFixture), t)
}

type OptionalMonitorsFixture struct {
	*gunit.Fixture
}

func (this *OptionalMonitorsFixture) TestMonitorWithoutExtensions_ResolvesToNop() {
	recovery, handoff := optionalMonitors(basicMonitor{})

	this.So(recovery, should.Equal, nop{})
	this.So(handoff, should.Equal, nop{})
}
func (this *OptionalMonitorsFixture) TestMonitorWithExtensions_ResolvesToTheMonitor() {
	monitor := &extendedMonitor{}

	recovery, handoff := optionalMonitors(monitor)

	this.So(recovery, should.Equal, monitor)
	this.So(handoff, should.Equal, monitor)
}

type basicMonitor struct{}

func (basicMonitor) MessageReceived(int)  {}
func (basicMonitor) MessageStored(int)    {}
func (basicMonitor) MessagePublished(int) {}
func (basicMonitor) MessageConfirmed(int) {}

type extendedMonitor struct{ basicMonitor }

func (*extendedMonitor) StartupRecovered(int)    {}
func (*extendedMonitor) HandoffDeferred(int)     {}
func (*extendedMonitor) HandoffCapacityReached() {}
