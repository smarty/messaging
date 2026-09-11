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
	ConnectionBlocked(reason string)
	ConnectionUnblocked()
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
	ErrAlreadyExclusive   = errors.New("unable to open additional stream, an exclusive stream already exists")
	ErrMultipleStreams    = errors.New("unable to open exclusive stream, another stream already exists")
	ErrCommitTimeout      = errors.New("the broker did not acknowledge the transaction within the commit timeout")
	ErrPublishTimeout     = errors.New("the broker did not accept the publish within the commit timeout")
	ErrAcknowledgeTimeout = errors.New("the broker did not accept the acknowledgement within the commit timeout")
	ErrCloseTimeout       = errors.New("the broker did not answer the channel close within the commit timeout")
	ErrInvalidHeader      = errors.New("a dispatch header value has a type the AMQP wire format cannot carry")
)
