package rabbitmq

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/messaging/v4"
	"github.com/smartystreets/messaging/v4/rabbitmq/adapter"
)

func TestConnectorFixture(t *testing.T) {
	gunit.Run(new(ConnectorFixture), t)
}

type ConnectorFixture struct {
	*gunit.Fixture

	brokerAddress string
	ctx           context.Context
	connector     messaging.Connector

	dialContext context.Context
	dialNetwork string
	dialAddress string
	dialError   error

	connectContext context.Context
	connectSocket  net.Conn
	connectConfig  adapter.Config
	connectError   error

	callsToClose int
}

func (this *ConnectorFixture) Setup() {
	this.ctx = context.Background()
	this.brokerAddress = "amqp://my-username:my-password@localhost:5672/my-vhost"
	this.initializeConnector()
}
func (this *ConnectorFixture) initializeConnector() {
	this.connector = New(
		Options.Address(this.brokerAddress),
		Options.Connector(this),
		Options.Dialer(this),
	)
}

func (this *ConnectorFixture) TestWhenConnectingToBroker_UseDialedNetworkConnectionAndParsedConfig() {
	connection, err := this.connector.Connect(this.ctx)

	this.So(connection, should.HaveSameTypeAs, &defaultConnection{})
	this.So(err, should.BeNil)

	this.So(this.dialContext, should.Equal, this.ctx)
	this.So(this.dialNetwork, should.Equal, "tcp")
	this.So(this.dialAddress, should.Equal, "localhost:5672")

	this.So(this.connectContext, should.Equal, this.ctx)
	this.So(this.connectSocket, should.Equal, this)
	this.So(this.connectConfig, should.Resemble, adapter.Config{
		Username:    "my-username",
		Password:    "my-password",
		VirtualHost: "/my-vhost",
	})
}
func (this *ConnectorFixture) TestWhenNoCredentialsFound_ConnectUsingDefaultCredentials() {
	this.brokerAddress = "amqp://localhost:5672/another-vhost"
	this.initializeConnector()

	connection, err := this.connector.Connect(this.ctx)

	this.So(connection, should.NotBeNil)
	this.So(err, should.BeNil)

	this.So(this.dialContext, should.Equal, this.ctx)
	this.So(this.dialNetwork, should.Equal, "tcp")
	this.So(this.dialAddress, should.Equal, "localhost:5672")

	this.So(this.connectContext, should.Equal, this.ctx)
	this.So(this.connectSocket, should.Equal, this)
	this.So(this.connectConfig, should.Resemble, adapter.Config{
		Username:    "guest",
		Password:    "guest",
		VirtualHost: "/another-vhost",
	})
}

func (this *ConnectorFixture) TestWhenDialingFails_ReturnUnderlyingError() {
	this.dialError = errors.New("")

	connection, err := this.connector.Connect(context.Background())

	this.So(connection, should.BeNil)
	this.So(err, should.Equal, this.dialError)
}
func (this *ConnectorFixture) TestWhenUnderlyingConnectorFails_ReturnUnderlyingError() {
	this.connectError = errors.New("")

	connection, err := this.connector.Connect(context.Background())

	this.So(connection, should.BeNil)
	this.So(err, should.Equal, this.connectError)
}

func (this *ConnectorFixture) TestCloseInvokesCloseOnAllTrackedConnections() {
	_, _ = this.connector.Connect(context.Background())

	_ = this.connector.Close()

	this.So(this.callsToClose, should.Equal, 1)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *ConnectorFixture) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	this.dialContext = ctx
	this.dialNetwork = network
	this.dialAddress = address
	return this, this.dialError
}
func (this *ConnectorFixture) Connect(ctx context.Context, socket net.Conn, config adapter.Config) (adapter.Connection, error) {
	this.connectContext = ctx
	this.connectSocket = socket
	this.connectConfig = config
	return this, this.connectError
}

func (this *ConnectorFixture) Close() error                      { this.callsToClose++; return nil }
func (this *ConnectorFixture) Channel() (adapter.Channel, error) { panic("nop") }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *ConnectorFixture) Read(b []byte) (n int, err error)   { panic("nop") }
func (this *ConnectorFixture) LocalAddr() net.Addr                { panic("nop") }
func (this *ConnectorFixture) RemoteAddr() net.Addr               { panic("nop") }
func (this *ConnectorFixture) SetDeadline(t time.Time) error      { panic("nop") }
func (this *ConnectorFixture) SetReadDeadline(t time.Time) error  { panic("nop") }
func (this *ConnectorFixture) SetWriteDeadline(t time.Time) error { panic("nop") }
