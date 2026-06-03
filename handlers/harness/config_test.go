package harness

import (
	"context"
	"sync"
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
)

func TestConfigFixture(t *testing.T) {
	gunit.Run(new(ConfigFixture), t)
}

type ConfigFixture struct {
	*gunit.Fixture
}

func (this *ConfigFixture) apply(options ...option) configuration {
	var cfg configuration
	for _, item := range Options.defaults(options...) {
		item(&cfg)
	}
	return cfg
}

func (this *ConfigFixture) TestNop() {
	var n nop
	this.So(func() { n.Track(nil) }, should.NOT.Panic)
	this.So(n.Serialize(nil, nil), should.BeNil)
	this.So(n.ContentType(), should.BeEmpty)
	this.So(n.Write(nil), should.BeNil)
	this.So(n.Dispatch(nil), should.BeNil)
}

func (this *ConfigFixture) TestDefaultsPopulateCapacities() {
	cfg := this.apply()
	this.So(cfg.BatchCapacity, should.Equal, 1024)
	this.So(cfg.UnitCapacity, should.Equal, 1)
	this.So(cfg.UnitSize, should.Equal, 64)
	this.So(cfg.SerializerCount, should.Equal, 4)
	this.So(cfg.ShedThreshold, should.Equal, 0.80)
}

func (this *ConfigFixture) TestDefaultCollaboratorsAreNop() {
	cfg := this.apply()
	this.So(cfg.Monitor, should.Equal, nop{})
	this.So(cfg.Serializer, should.Equal, nop{})
	this.So(cfg.Writer, should.Equal, nop{})
	this.So(cfg.Dispatcher, should.Equal, nop{})
}

func (this *ConfigFixture) TestTypesOptionStoresValuesVerbatim() {
	cfg := this.apply(Options.Types("a", 42, struct{}{}))
	this.So(cfg.Types, should.Equal, []any{"a", 42, struct{}{}})
}

func (this *ConfigFixture) TestTunableOptionsOverrideDefaults() {
	cfg := this.apply(
		Options.BurstCapacity(2),
		Options.PipelineBufferCapacity(2),
		Options.ExecutionUnitSize(8),
		Options.SerializerCount(3),
		Options.ShedThreshold(0.5),
	)
	this.So(cfg.BatchCapacity, should.Equal, 2)
	this.So(cfg.UnitCapacity, should.Equal, 2)
	this.So(cfg.UnitSize, should.Equal, 8)
	this.So(cfg.SerializerCount, should.Equal, 3)
	this.So(cfg.ShedThreshold, should.Equal, 0.5)
}

func (this *ConfigFixture) TestCollaboratorOptionsOverrideDefaults() {
	recorder := &recordingMonitor{}
	cfg := this.apply(Options.Monitor(recorder))
	this.So(cfg.Monitor, should.Equal, recorder)
}

func (this *ConfigFixture) TestZeroOptionsPipelineRunsInertly() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler, listeners := New(ctx)

	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for _, listener := range listeners {
			wg.Go(listener.Listen)
		}
		wg.Wait()
		close(done)
	}()

	handler.Handle(ctx, "payload")
	this.So(handler.(interface{ Close() error }).Close(), should.BeNil)
	<-done
}

type recordingMonitor struct{ observations []any }

func (this *recordingMonitor) Track(observation any) {
	this.observations = append(this.observations, observation)
}
