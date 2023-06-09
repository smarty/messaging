package rabbitmq

import (
	"context"
	"net/url"
	"strings"
	"sync"

	"github.com/smarty/messaging/v3"
	"github.com/smarty/messaging/v3/rabbitmq/adapter"
)

type defaultConnector struct {
	inner   adapter.Connector
	dialer  netDialer
	broker  brokerEndpoint
	config  configuration
	monitor monitor
	logger  logger

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

	var encryption = "plaintext"
	if this.broker.Address.Scheme == "amqps" {
		encryption = "encrypted"
	}

	this.logger.Printf("[INFO] Establishing [%s] AMQP connection with user [%s] to [%s://%s] using virtual host [%s]...", encryption, config.Username, this.broker.Address.Scheme, hostAddress, config.VirtualHost)
	socket, err := this.dialer.DialContext(ctx, "tcp", hostAddress)
	if err != nil {
		this.logger.Printf("[WARN] Unable to connect [%s].", err)
		this.monitor.ConnectionOpened(err)
		return nil, err
	}

	amqpConnection, err := this.inner.Connect(ctx, socket, config)
	if err != nil {
		this.logger.Printf("[WARN] Unable to connect [%s].", err)
		this.config.Monitor.ConnectionOpened(err)
		return nil, err
	}

	this.logger.Printf("[INFO] Established [%s] AMQP connection with user [%s] to [%s://%s] using virtual host [%s].", encryption, config.Username, this.broker.Address.Scheme, hostAddress, config.VirtualHost)
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.active = append(this.active, newConnection(amqpConnection, this.config))
	return this.active[len(this.active)-1], nil
}
func (this *defaultConnector) configuration() (string, adapter.Config) {
	query := this.broker.Address.Query()
	username, password := parseAuthentication(this.broker.Address.User, query.Get("username"), query.Get("password"))
	return this.broker.Address.Host, adapter.Config{
		Username:    username,
		Password:    password,
		VirtualHost: parseVirtualHost(this.broker.Address.Path),
	}
}
func parseAuthentication(info *url.Userinfo, queryUsername, queryPassword string) (string, string) {
	if info == nil {
		return coalesce(queryUsername, "guest"), coalesce(queryPassword, "guest")
	}

	username := coalesce(info.Username(), queryUsername)
	password, _ := info.Password()
	password = coalesce(password, queryPassword)

	return username, password
}
func parseVirtualHost(value string) string {
	value = strings.TrimPrefix(value, "/")
	if len(value) == 0 {
		return "/"
	}

	return value
}
func coalesce(values ...string) string {
	for _, value := range values {
		if len(value) > 0 {
			return value
		}
	}

	return ""
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
