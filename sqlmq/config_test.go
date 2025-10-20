package sqlmq

import (
	"testing"

	"github.com/smarty/gunit"
	"github.com/smarty/gunit/assert/should"
)

func TestConfigFixture(t *testing.T) {
	gunit.Run(new(ConfigFixture), t)
}

type ConfigFixture struct {
	*gunit.Fixture
}

func (this *ConfigFixture) TestPanicOnInvalidDriver() {
	config := configuration{}
	this.So(func() {
		Options.apply(Options.DataSource("", ""))(&config)
	}, should.Panic)
}
