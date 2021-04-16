package streaming

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/messaging/v4"
)

func TestConfigFixture(t *testing.T) {
	gunit.Run(new(ConfigFixture), t)
}

type ConfigFixture struct {
	*gunit.Fixture

	ctx      context.Context
	shutdown context.CancelFunc
	manager  messaging.ListenCloser

	readerContext context.Context
	readerError   error
}

func (this *ConfigFixture) Setup() {
	this.ctx, this.shutdown = context.WithCancel(context.Background())
	this.manager = New(this, Options.Subscriptions(SubscriptionOptions.AddWorkers(this)))
}

func (this *ConfigFixture) TestWhenManagerListens_UnderlyingWorkerStarted() {
	this.readerError = errors.New("")
	go func() {
		time.Sleep(time.Millisecond * 5)
		closeResource(this.manager)
	}()
	this.manager.Listen()

	this.So(this.readerContext, should.NotBeNil)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *ConfigFixture) Connect(ctx context.Context) (messaging.Connection, error) {
	return this, nil
}
func (this *ConfigFixture) Close() error {
	return nil
}

func (this *ConfigFixture) Reader(ctx context.Context) (messaging.Reader, error) {
	this.readerContext = ctx
	return nil, this.readerError
}
func (this *ConfigFixture) Writer(ctx context.Context) (messaging.Writer, error) {
	panic("nop")
}
func (this *ConfigFixture) CommitWriter(ctx context.Context) (messaging.CommitWriter, error) {
	panic("nop")
}

func (this *ConfigFixture) Handle(_ context.Context, _ ...interface{}) {
}
