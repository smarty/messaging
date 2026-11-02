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
	ErrAlreadyExclusive = errors.New("unable to open additional stream, an exclusive stream already exists")
	ErrMultipleStreams  = errors.New("unable to open exclusive stream, another stream already exists")
	ErrInvalidHeader    = errors.New("a dispatch header value has a type the AMQP wire format cannot carry")
)

// The broker-timeout sentinels report Timeout() == true, the convention net.Error
// uses, so a caller (a monitor, for example) can recognize every one of them
// without importing this package:
//
//	var timeout interface{ Timeout() bool }
//	if errors.As(err, &timeout) && timeout.Timeout() { ... }
//
// They remain comparable with == and errors.Is, as before.
var (
	ErrCommitTimeout      error = timeoutError("the broker did not acknowledge the transaction within the broker timeout")
	ErrPublishTimeout     error = timeoutError("the broker did not accept the publish within the broker timeout")
	ErrAcknowledgeTimeout error = timeoutError("the broker did not accept the acknowledgement within the broker timeout")
	ErrCloseTimeout       error = timeoutError("the channel close or consumer cancel did not complete within the broker timeout")
)

type timeoutError string

func (this timeoutError) Error() string { return string(this) }
func (this timeoutError) Timeout() bool { return true }
