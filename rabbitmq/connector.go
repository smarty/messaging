package rabbitmq

import (
	"context"
	"net/url"
	"sync"

	"github.com/smartystreets/messaging/v3"
	"github.com/smartystreets/messaging/v3/rabbitmq/adapter"
)

type defaultConnector struct {
	inner   adapter.Connector
	dialer  netDialer
	broker  func() BrokerEndpoint
	config  configuration
	monitor Monitor
	logger  messaging.Logger

	active []messaging.Connection
	mutex  sync.Mutex
}

func newConnector(config configuration) messaging.Connector {
	return &defaultConnector{
		inner:   config.Connector,
		dialer:  config.Dialer,
		broker:  config.Endpoint,
		config:  config,
		monitor: config.Monitor,
		logger:  config.Logger,
	}
}

func (this *defaultConnector) Connect(ctx context.Context) (messaging.Connection, error) {
	hostAddress, config := this.configuration()
	socket, err := this.dialer.DialContext(ctx, "tcp", hostAddress)
	if err != nil {
		this.logger.Println("[WARN] Unable to connect:", err)
		this.monitor.ConnectionOpened(err)
		return nil, err
	}

	amqpConnection, err := this.inner.Connect(ctx, socket, config)
	if err != nil {
		this.logger.Println("[WARN] Unable to connect:", err)
		this.config.Monitor.ConnectionOpened(err)
		return nil, err
	}

	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.active = append(this.active, newConnection(amqpConnection, this.config))
	return this.active[len(this.active)-1], nil
}
func (this *defaultConnector) configuration() (string, adapter.Config) {
	broker := this.broker()
	username, password := parseAuthentication(broker.Address.User)
	return broker.Address.Host, adapter.Config{
		Username:    username,
		Password:    password,
		VirtualHost: broker.Address.Path,
	}
}
func parseAuthentication(info *url.Userinfo) (string, string) {
	if info == nil {
		return "guest", "guest"
	}

	password, _ := info.Password()
	return info.Username(), password
}

func (this *defaultConnector) Close() error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	for i := range this.active {
		_ = this.active[i].Close()
		this.active[i] = nil
	}
	this.active = this.active[0:0]

	return nil
}
