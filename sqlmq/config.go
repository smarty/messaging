package sqlmq

import (
	"context"
	"database/sql"
	"time"

	"github.com/smarty/messaging/v4"
	"github.com/smarty/messaging/v4/batch"
	"github.com/smarty/messaging/v4/sqlmq/adapter"
)

type configuration struct {
	Context             context.Context
	Target              messaging.Connector
	DriverName          string
	DataSource          string
	StorageHandle       adapter.Handle
	Channel             chan messaging.Dispatch
	SQLTxOptions        sql.TxOptions
	AutoincrementStride uint64
	Now                 func() time.Time
	Sleep               time.Duration
	HandoffTimeout      time.Duration
	Logger              logger
	Monitor             monitor

	DeferredHandoffCapacity int
	Deferred                *deferredHandoffs

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
func (singleton) StorageHandle(value *sql.DB) option {
	return Options.StorageAdapter(adapter.New(value))
}
func (singleton) StorageAdapter(value adapter.Handle) option {
	return func(this *configuration) { this.StorageHandle = value }
}
func (singleton) Channel(value chan messaging.Dispatch) option {
	return func(this *configuration) { this.Channel = value }
}
func (singleton) ChannelBufferCapacity(value int) option {
	return func(this *configuration) { this.Channel = make(chan messaging.Dispatch, value) }
}
func (singleton) IsolationLevel(value sql.IsolationLevel) option {
	return func(this *configuration) { this.SQLTxOptions = sql.TxOptions{Isolation: value} }
}
func (singleton) AutoincrementStride(value uint8) option {
	return func(this *configuration) {
		if value == 0 {
			value = 1
		}

		this.AutoincrementStride = uint64(value)
	}
}
func (singleton) Now(value func() time.Time) option {
	return func(this *configuration) { this.Now = value }
}
func (singleton) RetryTimeout(value time.Duration) option {
	return func(this *configuration) { this.Sleep = value }
}

// HandoffTimeout bounds how long a committed outbox transaction waits to hand
// its messages to the dispatch processor. When the bound elapses, the messages
// that were not accepted move to a background handoff (see
// DeferredHandoffCapacity) and Commit returns success, because the rows are
// already durable. A zero or negative value is replaced with the default.
func (singleton) HandoffTimeout(value time.Duration) option {
	return func(this *configuration) { this.HandoffTimeout = sanitizeHandoffTimeout(value) }
}
func sanitizeHandoffTimeout(value time.Duration) time.Duration {
	if value <= 0 {
		return defaultHandoffTimeout
	}
	return value
}

// DeferredHandoffCapacity caps the number of committed messages that may be
// held in memory by background handoffs at once. When a new deferral would
// exceed the cap, Commit instead waits for the dispatch processor, which
// applies back-pressure to the caller. A value of 1 effectively disables
// deferral. A zero or negative value is replaced with the default.
func (singleton) DeferredHandoffCapacity(value int) option {
	return func(this *configuration) { this.DeferredHandoffCapacity = sanitizeDeferredHandoffCapacity(value) }
}
func sanitizeDeferredHandoffCapacity(value int) int {
	if value <= 0 {
		return defaultDeferredHandoffCapacity
	}
	return value
}
func (singleton) MessageStore(value messageStore) option {
	return func(this *configuration) { this.MessageStore = value }
}
func (singleton) MessageSender(value messaging.Writer) option {
	return func(this *configuration) { this.Sender = value }
}
func (singleton) Logger(value logger) option {
	return func(this *configuration) { this.Logger = value }
}
func (singleton) Monitor(value monitor) option {
	return func(this *configuration) { this.Monitor = value }
}

func (singleton) apply(options ...option) option {
	return func(this *configuration) {
		for _, item := range Options.defaults(options...) {
			item(this)
		}

		if this.StorageHandle == nil {
			this.StorageHandle = adapter.Open(this.DriverName, this.DataSource)
		}

		if this.MessageStore == nil {
			this.MessageStore = newMessageStore(this.StorageHandle, this.AutoincrementStride, this.Now)
		}

		if this.Sender == nil {
			this.Sender = batch.NewWriter(this.Target)
		}

		this.Deferred = newDeferredHandoffs(this.Context, this.Channel, this.DeferredHandoffCapacity)
	}
}
func (singleton) defaults(options ...option) []option {
	var defaultContext = context.Background()
	var defaultLogger = nop{}
	var defaultMonitor = nop{}
	const defaultChannelBufferCapacity = 1024
	const defaultIsolationLevel = sql.LevelReadCommitted
	const defaultRetryTimeout = time.Second * 5
	const defaultAutoincrementStride = 1

	return append([]option{
		Options.Context(defaultContext),
		Options.ChannelBufferCapacity(defaultChannelBufferCapacity),
		Options.IsolationLevel(defaultIsolationLevel),
		Options.AutoincrementStride(defaultAutoincrementStride),
		Options.Now(time.Now),
		Options.RetryTimeout(defaultRetryTimeout),
		Options.HandoffTimeout(defaultHandoffTimeout),
		Options.DeferredHandoffCapacity(defaultDeferredHandoffCapacity),
		Options.Logger(defaultLogger),
		Options.Monitor(defaultMonitor),
	}, options...)
}

const (
	defaultHandoffTimeout          = time.Second * 10
	defaultDeferredHandoffCapacity = 8192
)

type nop struct{}

func (nop) Printf(_ string, _ ...any) {}

func (nop) MessageReceived(_ int)  {}
func (nop) MessageStored(_ int)    {}
func (nop) MessagePublished(_ int) {}
func (nop) MessageConfirmed(_ int) {}
