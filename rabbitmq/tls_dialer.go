package rabbitmq

import (
	"context"
	"crypto/tls"
	"net"
	"time"
)

type (
	netDialer interface {
		DialContext(ctx context.Context, network, address string) (net.Conn, error)
	}
	tlsConn interface {
		net.Conn
		HandshakeContext(ctx context.Context) error
	}
	tlsClientFunc func(conn net.Conn, config *tls.Config) tlsConn
)

type tlsDialer struct {
	netDialer
	endpoint brokerEndpoint
	client   tlsClientFunc
	timeout  time.Duration
}

func newTLSDialer(dialer netDialer, config configuration) netDialer {
	return tlsDialer{netDialer: dialer, endpoint: config.Endpoint, client: config.TLSClient, timeout: config.BrokerTimeout}
}

// DialContext dials and, for amqps, completes the TLS handshake. A peer that
// accepts TCP and then never answers the handshake would otherwise hang the
// caller forever, so the handshake runs under the caller's deadline, or under
// BrokerTimeout when the caller set none.
func (this tlsDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, this.timeout)
		defer cancel()
	}

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
	if err = tlsConnection.HandshakeContext(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}

	return tlsConnection, nil
}
