package rabbitmq

import (
	"context"
	"crypto/tls"
	"net"
)

type (
	netDialer interface {
		DialContext(ctx context.Context, network, address string) (net.Conn, error)
	}
	tlsConn interface {
		net.Conn
		Handshake() error
	}
	tlsClientFunc func(conn net.Conn, config *tls.Config) tlsConn
)

type tlsDialer struct {
	netDialer
	endpoint brokerEndpoint
	client   tlsClientFunc
}

func newTLSDialer(dialer netDialer, config configuration) netDialer {
	return tlsDialer{netDialer: dialer, endpoint: config.Endpoint, client: config.TLSClient}
}

func (this tlsDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := this.netDialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}

	if this.endpoint.TLSConfig == nil || this.endpoint.Address.Scheme != "amqps" {
		return conn, nil
	}

	if len(this.endpoint.TLSConfig.ServerName) == 0 {
		this.endpoint.TLSConfig.ServerName, _, _ = net.SplitHostPort(this.endpoint.Address.Host)
	}

	tlsConnection := this.client(conn, this.endpoint.TLSConfig)
	if err = tlsConnection.Handshake(); err != nil {
		_ = conn.Close()
		return nil, err
	}

	return tlsConnection, nil
}
