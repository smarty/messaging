package rabbitmq

import (
	"crypto/tls"
	"net"
	"net/url"
	"time"

	"github.com/smartystreets/messaging/v3"
	"github.com/smartystreets/messaging/v3/rabbitmq/adapter"
)

func New(options ...option) messaging.Connector {
	var config configuration
	Options.apply(options...)(&config)
	return newConnector(config)
}

type configuration struct {
	Address              url.URL
	TLSConfig            *tls.Config
	Endpoint             brokerEndpoint
	TLSClient            tlsClientFunc
	Dialer               netDialer
	Connector            adapter.Connector
	Logger               logger
	Monitor              monitor
	Now                  func() time.Time
	TopologyFailurePanic bool
}

var Options singleton

type singleton struct{}
type option func(*configuration)

func (singleton) Address(value string) option {
	return func(this *configuration) { address, _ := url.Parse(value); this.Address = *address }
}
func (singleton) TLSConfig(value *tls.Config) option {
	return func(this *configuration) { this.TLSConfig = value }
}
func (singleton) Connector(value adapter.Connector) option {
	return func(this *configuration) { this.Connector = value }
}
func (singleton) Dialer(value netDialer) option {
	return func(this *configuration) { this.Dialer = value }
}
func (singleton) TLSClient(value tlsClientFunc) option {
	return func(this *configuration) { this.TLSClient = value }
}
func (singleton) PanicOnTopologyError(value bool) option {
	return func(this *configuration) { this.TopologyFailurePanic = value }
}
func (singleton) Logger(value logger) option {
	return func(this *configuration) { this.Logger = value }
}
func (singleton) Monitor(value monitor) option {
	return func(this *configuration) { this.Monitor = value }
}
func (singleton) Now(value func() time.Time) option {
	return func(this *configuration) { this.Now = value }
}
func (singleton) apply(options ...option) option {
	return func(this *configuration) {
		for _, item := range Options.defaults(options...) {
			item(this)
		}

		this.Endpoint = brokerEndpoint{Address: this.Address, TLSConfig: this.TLSConfig}

		if this.TLSClient == nil {
			this.TLSClient = this.defaultTLSClient
		}

		if this.Dialer == nil {
			this.Dialer = this.defaultDialer()
		}
	}
}
func (singleton) defaults(options ...option) []option {
	const defaultAddress = "amqp://guest:guest@127.0.0.1:5672/"
	const defaultTopologyFailurePanic = true
	var defaultNow = time.Now
	var defaultLogger = nop{}
	var defaultMonitor = nop{}
	var defaultTLS = &tls.Config{
		MinVersion:             tls.VersionTLS12,
		SessionTicketsDisabled: true,
		CipherSuites: []uint16{
			tls.TLS_FALLBACK_SCSV,

			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, // TLS v1.2
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,   // TLS v1.2

			tls.TLS_AES_128_GCM_SHA256,       // TLS v1.3
			tls.TLS_AES_256_GCM_SHA384,       // TLS v1.3
			tls.TLS_CHACHA20_POLY1305_SHA256, // TLS v1.3
		},
	}

	return append([]option{
		Options.Address(defaultAddress),
		Options.TLSConfig(defaultTLS),
		Options.Connector(adapter.New()),
		Options.PanicOnTopologyError(defaultTopologyFailurePanic),
		Options.Logger(defaultLogger),
		Options.Monitor(defaultMonitor),
		Options.Now(defaultNow),
	}, options...)
}

func (this configuration) defaultTLSClient(conn net.Conn, config *tls.Config) tlsConn {
	return tls.Client(conn, config)
}
func (this configuration) defaultDialer() netDialer {
	return newTLSDialer(&net.Dialer{}, this)
}

type nop struct{}

func (nop) Printf(_ string, _ ...interface{}) {}

func (nop) ConnectionOpened(_ error)               {}
func (nop) ConnectionClosed()                      {}
func (nop) DispatchPublished()                     {}
func (nop) DeliveryReceived()                      {}
func (nop) DeliveryAcknowledged(_ uint16, _ error) {}
func (nop) TransactionCommitted(_ error)           {}
func (nop) TransactionRolledBack(_ error)          {}
