package rabbitmq

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/smarty/gunit"
	"github.com/smarty/gunit/assert/should"
	"github.com/smarty/messaging/v4"
	"github.com/smarty/messaging/v4/rabbitmq/adapter"
)

func TestConnectorFixture(t *testing.T) {
	gunit.Run(new(ConnectorFixture), t)
}

// expectedDefaultHeartbeat pins the default-heartbeat contract independently
// of the production constant; a change to the default must fail here once.
const expectedDefaultHeartbeat = time.Second * 10

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
func (this *ConnectorFixture) Teardown() {
	_ = this.connector.Close() // release the goroutines parked behind each connection
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
	this.So(this.connectConfig, should.Equal, adapter.Config{
		Username:    "my-username",
		Password:    "my-password",
		VirtualHost: "my-vhost",
		Heartbeat:   expectedDefaultHeartbeat,
	})
}
func (this *ConnectorFixture) TestCredentialsFromQueryString() {
	this.brokerAddress = "amqp://localhost:5672/the-vhost?username=My-Username-1&password=My-Password-1"
	this.initializeConnector()
	_, _ = this.connector.Connect(this.ctx)

	this.So(this.connectConfig, should.Equal, adapter.Config{
		Username:    "My-Username-1",
		Password:    "My-Password-1",
		VirtualHost: "the-vhost",
		Heartbeat:   expectedDefaultHeartbeat,
	})
}
func (this *ConnectorFixture) TestCredentialsFromQueryString_PreferUserInfo() {
	this.brokerAddress = "amqp://username-1:password-1@localhost:5672/the-vhost?username=username-2&password=password-2"
	this.initializeConnector()
	_, _ = this.connector.Connect(this.ctx)

	this.So(this.connectConfig, should.Equal, adapter.Config{
		Username:    "username-1",
		Password:    "password-1",
		VirtualHost: "the-vhost",
		Heartbeat:   expectedDefaultHeartbeat,
	})
}
func (this *ConnectorFixture) TestConfiguredHeartbeatOverridesDefault() {
	this.assertConfiguredHeartbeat(30*time.Second, 30*time.Second)
}
func (this *ConnectorFixture) TestZeroHeartbeat_DefersToTheBroker() {
	this.assertConfiguredHeartbeat(0, 0)
}
func (this *ConnectorFixture) TestNegativeHeartbeat_ReplacedByTheDefault() {
	this.assertConfiguredHeartbeat(-time.Second, expectedDefaultHeartbeat)
}
func (this *ConnectorFixture) TestSubSecondHeartbeat_RoundsUpToOneSecond() {
	this.assertConfiguredHeartbeat(900*time.Millisecond, time.Second)
}
func (this *ConnectorFixture) assertConfiguredHeartbeat(configured, expected time.Duration) {
	this.connector = New(
		Options.Address(this.brokerAddress),
		Options.Connector(this),
		Options.Dialer(this),
		Options.Heartbeat(configured),
	)

	_, _ = this.connector.Connect(this.ctx)

	this.So(this.connectConfig.Heartbeat, should.Equal, expected)
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
	this.So(this.connectConfig, should.Equal, adapter.Config{
		Username:    "guest",
		Password:    "guest",
		VirtualHost: "another-vhost",
		Heartbeat:   expectedDefaultHeartbeat,
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

func (this *ConnectorFixture) Close() error                               { this.callsToClose++; return nil }
func (this *ConnectorFixture) Channel() (adapter.Channel, error)          { panic("nop") }
func (this *ConnectorFixture) BlockedNotifications() <-chan amqp.Blocking { return nil }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *ConnectorFixture) Read(b []byte) (n int, err error)   { panic("nop") }
func (this *ConnectorFixture) LocalAddr() net.Addr                { panic("nop") }
func (this *ConnectorFixture) RemoteAddr() net.Addr               { panic("nop") }
func (this *ConnectorFixture) SetDeadline(t time.Time) error      { panic("nop") }
func (this *ConnectorFixture) SetReadDeadline(t time.Time) error  { panic("nop") }
func (this *ConnectorFixture) SetWriteDeadline(t time.Time) error { panic("nop") }
