package sqlmq

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/smarty/gunit"
	"github.com/smarty/gunit/assert/should"
	"github.com/smarty/messaging/v4"
)

func TestConfigFixture(t *testing.T) {
	gunit.Run(new(ConfigFixture), t)
}

type ConfigFixture struct {
	*gunit.Fixture
	config configuration
}

func (this *ConfigFixture) apply(options ...option) {
	Options.apply(append([]option{Options.StorageHandle(&sql.DB{})}, options...)...)(&this.config)
}

func (this *ConfigFixture) TestPanicOnInvalidDriver() {
	config := configuration{}
	this.So(func() {
		Options.apply(Options.DataSource("", ""))(&config)
	}, should.Panic)
}

func (this *ConfigFixture) TestWhenHandoffTimeoutNotSpecified_UseDefault() {
	this.apply()
	this.So(this.config.HandoffTimeout, should.Equal, 10*time.Second)
}
func (this *ConfigFixture) TestWhenHandoffTimeoutZero_UseDefault() {
	this.apply(Options.HandoffTimeout(0))
	this.So(this.config.HandoffTimeout, should.Equal, defaultHandoffTimeout)
}
func (this *ConfigFixture) TestWhenHandoffTimeoutNegative_UseDefault() {
	this.apply(Options.HandoffTimeout(-time.Second))
	this.So(this.config.HandoffTimeout, should.Equal, defaultHandoffTimeout)
}
func (this *ConfigFixture) TestWhenHandoffTimeoutPositive_KeepValue() {
	this.apply(Options.HandoffTimeout(time.Minute))
	this.So(this.config.HandoffTimeout, should.Equal, time.Minute)
}

func (this *ConfigFixture) TestWhenDeferredHandoffCapacityNotSpecified_UseDefault() {
	this.apply()
	this.So(this.config.DeferredHandoffCapacity, should.Equal, 8192)
}
func (this *ConfigFixture) TestWhenDeferredHandoffCapacityZero_UseDefault() {
	this.apply(Options.DeferredHandoffCapacity(0))
	this.So(this.config.DeferredHandoffCapacity, should.Equal, defaultDeferredHandoffCapacity)
}
func (this *ConfigFixture) TestWhenDeferredHandoffCapacityNegative_UseDefault() {
	this.apply(Options.DeferredHandoffCapacity(-1))
	this.So(this.config.DeferredHandoffCapacity, should.Equal, defaultDeferredHandoffCapacity)
}
func (this *ConfigFixture) TestWhenDeferredHandoffCapacityPositive_KeepValue() {
	this.apply(Options.DeferredHandoffCapacity(42))
	this.So(this.config.DeferredHandoffCapacity, should.Equal, 42)
}

func (this *ConfigFixture) TestWhenApplied_CreateOneDeferredHandoffTrackerBoundToTheConfiguredChannelAndContext() {
	channel := make(chan messaging.Dispatch, 2)
	ctx := context.WithValue(context.Background(), lifetimeMarker{}, true)

	this.apply(Options.Channel(channel), Options.DeferredHandoffCapacity(42), Options.Context(ctx))

	this.So(this.config.Deferred, should.NotBeNil)
	this.So(this.config.Deferred.output, should.Equal, channel)
	this.So(this.config.Deferred.capacity, should.Equal, 42)
	this.So(this.config.Deferred.Context().Value(lifetimeMarker{}), should.Equal, true)
}

type lifetimeMarker struct{}
