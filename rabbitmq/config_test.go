package rabbitmq

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/smarty/gunit"
	"github.com/smarty/gunit/assert/should"
)

func TestConfigFixture(t *testing.T) {
	gunit.Run(new(ConfigFixture), t)
}

type ConfigFixture struct {
	*gunit.Fixture
	config configuration
}

func (this *ConfigFixture) TestWhenCallingDefaultTLSConnector_UseStandardLibraryTLS() {
	Options.apply()(&this.config)
	conn := this.config.TLSClient(nil, nil)
	this.So(conn, should.HaveSameTypeAs, &tls.Conn{})
}

func (this *ConfigFixture) TestWhenCommitTimeoutNotSpecified_UseDefault() {
	Options.apply()(&this.config)
	this.So(this.config.CommitTimeout, should.Equal, 30*time.Second)
}
func (this *ConfigFixture) TestWhenCommitTimeoutZero_UseDefault() {
	Options.apply(Options.CommitTimeout(0))(&this.config)
	this.So(this.config.CommitTimeout, should.Equal, defaultCommitTimeout)
}
func (this *ConfigFixture) TestWhenCommitTimeoutNegative_UseDefault() {
	Options.apply(Options.CommitTimeout(-time.Second))(&this.config)
	this.So(this.config.CommitTimeout, should.Equal, defaultCommitTimeout)
}
func (this *ConfigFixture) TestWhenCommitTimeoutPositive_KeepValue() {
	Options.apply(Options.CommitTimeout(time.Minute))(&this.config)
	this.So(this.config.CommitTimeout, should.Equal, time.Minute)
}
