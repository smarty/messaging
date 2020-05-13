package rabbitmq

import (
	"crypto/tls"
	"testing"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
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
