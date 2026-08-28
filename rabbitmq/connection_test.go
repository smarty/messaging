package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/smarty/gunit"
	"github.com/smarty/gunit/assert/should"
	"github.com/smarty/messaging/v4"
	"github.com/smarty/messaging/v4/rabbitmq/adapter"
)

func TestConnectionFixture(t *testing.T) {
	gunit.Run(new(ConnectionFixture), t)
}

type ConnectionFixture struct {
	*gunit.Fixture

	connection messaging.Connection

	txError      error
	channelError error
	closeError   error
	txCalls      int

	blocking chan amqp.Blocking
}

func (this *ConnectionFixture) Setup() {
	this.blocking = make(chan amqp.Blocking, 4)
	this.connection = newConnection(this, configuration{Monitor: nop{}, Logger: nop{}})
}

func (this *ConnectionFixture) TestWhenOpeningReader_OpenAChannelAndReturnReader() {
	reader, err := this.connection.Reader(context.Background())

	this.So(reader, should.HaveSameTypeAs, &defaultReader{})
	this.So(err, should.BeNil)
}
func (this *ConnectionFixture) TestWhenOpeningReaderFails_ReturnUnderlyingError() {
	this.channelError = errors.New("")

	reader, err := this.connection.Reader(context.Background())

	this.So(reader, should.BeNil)
	this.So(err, should.Equal, this.channelError)
}

func (this *ConnectionFixture) TestWhenOpeningCommitWriter_OpenATransactionalChannelAndReturnWriter() {
	writer, err := this.connection.CommitWriter(context.Background())

	this.So(writer, should.HaveSameTypeAs, defaultWriter{})
	this.So(err, should.BeNil)
	this.So(this.txCalls, should.Equal, 1)
}
func (this *ConnectionFixture) TestWhenOpeningChannelForCommitWriterFails_ReturnUnderlyingError() {
	this.channelError = errors.New("")

	writer, err := this.connection.CommitWriter(context.Background())

	this.So(writer, should.BeNil)
	this.So(err, should.Equal, this.channelError)
	this.So(this.txCalls, should.Equal, 0)
}
func (this *ConnectionFixture) TestWhenMarkingChannelAsTransactionalFails_ReturnUnderlyingError() {
	this.txError = errors.New("")

	writer, err := this.connection.CommitWriter(context.Background())

	this.So(writer, should.BeNil)
	this.So(err, should.Equal, this.txError)
	this.So(this.txCalls, should.Equal, 1)
}

func (this *ConnectionFixture) TestWhenOpeningWriter_OpenATransactionalChannelAndReturnWriter() {
	writer, err := this.connection.Writer(context.Background())

	this.So(writer, should.HaveSameTypeAs, defaultWriter{})
	this.So(err, should.BeNil)
}
func (this *ConnectionFixture) TestWhenOpeningChannelForWriterFails_ReturnUnderlyingError() {
	this.channelError = errors.New("")

	writer, err := this.connection.Writer(context.Background())

	this.So(writer, should.BeNil)
	this.So(err, should.Equal, this.channelError)
}

func (this *ConnectionFixture) TestWhenBrokerBlocksConnection_LogWarningWithReason() {
	logs := &capturingLogger{lines: make(chan string, 4)}
	this.connection = newConnection(this, configuration{Monitor: nop{}, Logger: logs})

	this.blocking <- amqp.Blocking{Active: true, Reason: "low memory"}

	line := receive(logs.lines)
	this.So(line, should.ContainSubstring, "[WARN]")
	this.So(line, should.ContainSubstring, "low memory")
}

func (this *ConnectionFixture) TestWhenBrokerUnblocksConnection_LogInfo() {
	logs := &capturingLogger{lines: make(chan string, 4)}
	this.connection = newConnection(this, configuration{Monitor: nop{}, Logger: logs})

	this.blocking <- amqp.Blocking{Active: false}

	line := receive(logs.lines)
	this.So(line, should.ContainSubstring, "[INFO]")
	this.So(line, should.ContainSubstring, "unblocked")
}

func (this *ConnectionFixture) TestWhenBrokerBlocksConnection_NotifyMonitorBlockedThenUnblockedInOrder() {
	monitor := &blockingMonitor{calls: make(chan string, 4)}
	this.connection = newConnection(this, configuration{Monitor: monitor, Logger: nop{}})

	this.blocking <- amqp.Blocking{Active: true, Reason: "low memory"}
	this.So(receive(monitor.calls), should.Equal, "blocked:low memory")

	this.blocking <- amqp.Blocking{Active: false}
	this.So(receive(monitor.calls), should.Equal, "unblocked")
}

func (this *ConnectionFixture) TestWhenMonitorCallbackBlocks_NotificationDeliveryContinues() {
	gate := make(chan struct{})
	monitor := &blockingMonitor{calls: make(chan string, 8), gate: gate}
	this.connection = newConnection(this, configuration{Monitor: monitor, Logger: nop{}})

	sent := make(chan struct{})
	go func() {
		defer close(sent)
		this.blocking <- amqp.Blocking{Active: true, Reason: "one"}
		this.blocking <- amqp.Blocking{Active: false}
		this.blocking <- amqp.Blocking{Active: true, Reason: "three"}
	}()

	completed := false
	select {
	case <-sent:
		completed = true
	case <-time.After(time.Millisecond * 100):
	}
	close(gate)

	this.So(completed, should.BeTrue)
}

func (this *ConnectionFixture) TestWhenMonitorCallbackIsSlow_LatestBlockedStateIsStillDelivered() {
	gate := make(chan struct{})
	monitor := &blockingMonitor{calls: make(chan string, 8), gate: gate}
	this.connection = newConnection(this, configuration{Monitor: monitor, Logger: nop{}})

	this.blocking <- amqp.Blocking{Active: true, Reason: "first"}
	this.So(receive(monitor.calls), should.Equal, "blocked:first") // watcher is now parked in the callback

	this.blocking <- amqp.Blocking{Active: false}
	this.blocking <- amqp.Blocking{Active: true, Reason: "final"}
	close(gate)

	this.So(receiveLast(monitor.calls), should.Equal, "blocked:final")
}

func (this *ConnectionFixture) TestWhenNotificationChannelCloses_WatcherStopsWithoutFurtherCallbacks() {
	monitor := &blockingMonitor{calls: make(chan string, 4)}
	this.connection = newConnection(this, configuration{Monitor: monitor, Logger: nop{}})

	this.blocking <- amqp.Blocking{Active: true, Reason: "low memory"}
	this.So(receive(monitor.calls), should.Equal, "blocked:low memory")

	close(this.blocking)

	this.So(receive(monitor.calls), should.Equal, "")
}

func (this *ConnectionFixture) TestWhenConnectionCloses_WatcherStopsEvenWhenNotificationChannelStaysOpen() {
	monitor := &blockingMonitor{calls: make(chan string, 4)}
	this.connection = newConnection(this, configuration{Monitor: monitor, Logger: nop{}})

	_ = this.connection.Close()
	this.blocking <- amqp.Blocking{Active: true, Reason: "after close"}

	this.So(receive(monitor.calls), should.Equal, "")
}

func (this *ConnectionFixture) TestWhenClosing_InvokeUnderlyingConnection() {
	this.closeError = errors.New("")

	err := this.connection.Close()

	this.So(err, should.Equal, this.closeError)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *ConnectionFixture) Channel() (adapter.Channel, error) { return this, this.channelError }
func (this *ConnectionFixture) Close() error                      { return this.closeError }
func (this *ConnectionFixture) NotifyBlocked(receiver chan amqp.Blocking) chan amqp.Blocking {
	this.blocking = receiver
	return receiver
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *ConnectionFixture) Tx() error { this.txCalls++; return this.txError }

func (this *ConnectionFixture) DeclareQueue(name string, replicated bool) error   { panic("nop") }
func (this *ConnectionFixture) DeclareExchange(name string) error                 { panic("nop") }
func (this *ConnectionFixture) BindQueue(queue, exchange string) error            { panic("nop") }
func (this *ConnectionFixture) BufferCapacity(value uint16) error                 { panic("nop") }
func (this *ConnectionFixture) Consume(_, _ string) (<-chan amqp.Delivery, error) { panic("nop") }
func (this *ConnectionFixture) Ack(deliveryTag uint64, multiple bool) error       { panic("nop") }
func (this *ConnectionFixture) CancelConsumer(consumerID string) error            { panic("nop") }
func (this *ConnectionFixture) Publish(_, _ string, _ amqp.Publishing) error      { panic("nop") }
func (this *ConnectionFixture) TxCommit() error                                   { panic("nop") }
func (this *ConnectionFixture) TxRollback() error                                 { panic("nop") }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type blockingMonitor struct {
	nop
	calls chan string
	gate  chan struct{} // when non-nil, every callback parks here after recording
}

func (this *blockingMonitor) ConnectionBlocked(reason string) { this.record("blocked:" + reason) }
func (this *blockingMonitor) ConnectionUnblocked()            { this.record("unblocked") }
func (this *blockingMonitor) record(call string) {
	this.calls <- call
	if this.gate != nil {
		<-this.gate
	}
}

type capturingLogger struct{ lines chan string }

func (this *capturingLogger) Printf(format string, args ...any) {
	this.lines <- fmt.Sprintf(format, args...)
}

func receive(values chan string) string {
	select {
	case value := <-values:
		return value
	case <-time.After(time.Millisecond * 100):
		return ""
	}
}
func receiveLast(values chan string) (result string) {
	for {
		value := receive(values)
		if value == "" {
			return result
		}
		result = value
	}
}
