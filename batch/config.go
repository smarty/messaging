package batch

import "github.com/smarty/messaging/v3"

type configuration struct {
	Connector   messaging.Connector
	ReuseWriter bool
}

func NewWriter(transport messaging.Connector, options ...option) messaging.Writer {
	var config configuration
	options = append(options, Options.Connector(transport))
	Options.apply(options...)(&config)
	return newWriter(config)
}

var Options singleton

type singleton struct{}
type option func(*configuration)

// Connector indicates the mechanism used to establish a connection to a messaging system.
func (singleton) Connector(value messaging.Connector) option {
	return func(this *configuration) { this.Connector = value }
}

// ReuseWriter indicates whether the requested writer can be safely reused. For example,
// if the underlying writer returned by the Connector provided is a "RabbitMQ" writer, it can
// be created and used until no longer needed or until the underlying connection becomes
// unstable. In contrast, if the underlying Connector interacts with a database, e.g. "sqlmq",
// via a transaction, then the writer that was created can only be use once because the
// underlying database transaction will commit when the batch of messages provided to the
// Write method have been fully written. After the transaction is committed, the instance of
// that Writer has concluded and must be thrown away.
// In most cases, ReuseWriter should use the default value of true.
func (singleton) ReuseWriter(value bool) option {
	return func(this *configuration) { this.ReuseWriter = value }
}

// Deprecated: CloseConnector is deprecated
func (singleton) CloseConnector(bool) option {
	return func(this *configuration) {} /* no-op */
}

func (singleton) apply(options ...option) option {
	return func(this *configuration) {
		for _, item := range Options.defaults(options...) {
			item(this)
		}
	}
}
func (singleton) defaults(options ...option) []option {
	return append([]option{
		Options.ReuseWriter(true),
	}, options...)
}
