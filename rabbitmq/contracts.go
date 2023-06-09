package rabbitmq

import (
	"crypto/tls"
	"errors"
	"net/url"
)

type brokerEndpoint struct {
	Address   *url.URL
	TLSConfig *tls.Config
}

type monitor interface {
	ConnectionOpened(error)
	ConnectionClosed()
	DispatchPublished()
	DeliveryReceived()
	DeliveryAcknowledged(uint16, error)
	TransactionCommitted(error)
	TransactionRolledBack(error)
}
type logger interface {
	Printf(format string, args ...any)
}

var (
	ErrAlreadyExclusive = errors.New("unable to open additional stream, an exclusive stream already exists")
	ErrMultipleStreams  = errors.New("unable to open exclusive stream, another stream already exists")
)
