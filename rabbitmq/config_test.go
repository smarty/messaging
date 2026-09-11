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

func (this *ConfigFixture) TestWhenBrokerTimeoutNotSpecified_UseDefault() {
	Options.apply()(&this.config)
	this.So(this.config.BrokerTimeout, should.Equal, 30*time.Second)
}
func (this *ConfigFixture) TestWhenBrokerTimeoutZero_UseDefault() {
	Options.apply(Options.BrokerTimeout(0))(&this.config)
	this.So(this.config.BrokerTimeout, should.Equal, defaultBrokerTimeout)
}
func (this *ConfigFixture) TestWhenBrokerTimeoutNegative_UseDefault() {
	Options.apply(Options.BrokerTimeout(-time.Second))(&this.config)
	this.So(this.config.BrokerTimeout, should.Equal, defaultBrokerTimeout)
}
func (this *ConfigFixture) TestWhenBrokerTimeoutPositive_KeepValue() {
	Options.apply(Options.BrokerTimeout(time.Minute))(&this.config)
	this.So(this.config.BrokerTimeout, should.Equal, time.Minute)
}
