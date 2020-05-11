package sqlmq

import (
	"context"
	"database/sql"
	"sync/atomic"
	"time"

	"github.com/smartystreets/messaging/v3"
	"github.com/smartystreets/messaging/v3/sqlmq/adapter"
)

type configuration struct {
	Context       context.Context
	Target        messaging.Connector
	DriverName    string
	DataSource    string
	StorageHandle adapter.Handle
	Channel       chan messaging.Dispatch
	SQLTxOptions  sql.TxOptions
	Now           func() time.Time
	Sleep         time.Duration
	Logger        messaging.Logger
	Monitor       Monitor

	MessageStore messageStore
	Sender       messaging.Writer
}

func New(transport messaging.Connector, options ...option) (messaging.Connector, messaging.ListenCloser) {
	var config configuration
	options = append(options, Options.TransportConnector(transport))
	Options.apply(options...)(&config)
	return newConnector(config), newDispatchProcessor(config)
}

var Options singleton

type singleton struct{}
type option func(*configuration)

func (singleton) Context(value context.Context) option {
	return func(this *configuration) { this.Context = value }
}
func (singleton) TransportConnector(value messaging.Connector) option {
	return func(this *configuration) { this.Target = value }
}
func (singleton) DataSource(driver, dataSource string) option {
	return func(this *configuration) { this.DriverName = driver; this.DataSource = dataSource }
}
func (singleton) DynamicStorageHandle(value atomic.Value) option {
	return func(this *configuration) { this.StorageHandle = newDynamicHandle(value) }
}
func (singleton) StorageHandle(value *sql.DB) option {
	return func(this *configuration) { this.StorageHandle = adapter.New(value) }
}
func (singleton) Channel(value chan messaging.Dispatch) option {
	return func(this *configuration) { this.Channel = value }
}
func (singleton) ChannelBufferSize(value int) option {
	return func(this *configuration) { this.Channel = make(chan messaging.Dispatch, value) }
}
func (singleton) IsolationLevel(value sql.IsolationLevel) option {
	return func(this *configuration) { this.SQLTxOptions = sql.TxOptions{Isolation: value} }
}
func (singleton) Now(value func() time.Time) option {
	return func(this *configuration) { this.Now = value }
}
func (singleton) RetryTimeout(value time.Duration) option {
	return func(this *configuration) { this.Sleep = value }
}
func (singleton) MessageStore(value messageStore) option {
	return func(this *configuration) { this.MessageStore = value }
}
func (singleton) MessageSender(value messaging.Writer) option {
	return func(this *configuration) { this.Sender = value }
}
func (singleton) Logger(value messaging.Logger) option {
	return func(this *configuration) { this.Logger = value }
}
func (singleton) Monitor(value Monitor) option {
	return func(this *configuration) { this.Monitor = value }
}

func (singleton) apply(options ...option) option {
	return func(this *configuration) {
		for _, option := range Options.defaults(options...) {
			option(this)
		}

		if this.StorageHandle == nil {
			this.StorageHandle = adapter.Open(this.DriverName, this.DataSource)
		}

		if this.MessageStore == nil {
			this.MessageStore = newMessageStore(this.StorageHandle, this.Now)
		}

		if this.Sender == nil {
			this.Sender = newDispatchSender(*this)
		}
	}
}
func (singleton) defaults(options ...option) []option {
	var defaultContext = context.Background()
	var defaultLogger = nop{}
	var defaultMonitor = nop{}
	const defaultChannelBufferSize = 1024
	const defaultIsolationLevel = sql.LevelReadCommitted
	const defaultRetryTimeout = time.Second * 5

	return append([]option{
		Options.Context(defaultContext),
		Options.ChannelBufferSize(defaultChannelBufferSize),
		Options.IsolationLevel(defaultIsolationLevel),
		Options.Now(time.Now),
		Options.RetryTimeout(defaultRetryTimeout),
		Options.Logger(defaultLogger),
		Options.Monitor(defaultMonitor),
	}, options...)
}

type nop struct{}

func (nop) Printf(_ string, _ ...interface{}) {}
func (nop) Println(_ ...interface{})          {}

func (nop) MessageReceived(_ int)  {}
func (nop) MessageStored(_ int)    {}
func (nop) MessagePublished(_ int) {}
func (nop) MessageConfirmed(_ int) {}
