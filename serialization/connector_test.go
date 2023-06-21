package serialization

import (
	"context"
	"errors"
	"testing"

	"github.com/smarty/messaging/v3"
	"github.com/smarty/assertions/should"
	"github.com/smarty/gunit"
)

func TestConnectorFixture(t *testing.T) {
	gunit.Run(new(ConnectorFixture), t)
}

type ConnectorFixture struct {
	*gunit.Fixture

	connector messaging.Connector

	originalContext     context.Context
	connectContext      context.Context
	readerContext       context.Context
	writerContext       context.Context
	commitWriterContext context.Context
	streamContext       context.Context
	streamReadContext   context.Context
	streamAckContext    context.Context
	writeContext        context.Context

	connectError      error
	readerError       error
	writerError       error
	commitWriterError error
	closeError        error
	streamError       error
	streamReadError   error
	streamAckError    error
	encodeError       error
	decodeError       error
	commitError       error
	rollbackError     error
	writeError        error

	streamConfig        messaging.StreamConfig
	streamReadDelivery  *messaging.Delivery
	streamAckDeliveries []messaging.Delivery
	writeDispatches     []messaging.Dispatch
}

func (this *ConnectorFixture) Setup() {
	this.originalContext = context.Background()
	this.connector = New(this, Options.Decoder(this), Options.Encoder(this))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *ConnectorFixture) TestWhenOpeningConnectionFails_ReturnUnderlyingError() {
	this.connectError = errors.New("")

	_, err := this.connector.Connect(this.originalContext)

	this.So(err, should.Equal, this.connectError)
	this.So(this.connectContext, should.Equal, this.originalContext)
}
func (this *ConnectorFixture) TestWhenOpeningReaderFails_ReturnUnderlyingError() {
	this.readerError = errors.New("")

	connection, connectError := this.connector.Connect(this.originalContext)
	_, err := connection.Reader(this.originalContext)

	this.So(connectError, should.BeNil)
	this.So(err, should.Equal, this.readerError)
	this.So(this.readerContext, should.Equal, this.originalContext)
}
func (this *ConnectorFixture) TestWhenOpeningWriterFails_ReturnUnderlyingError() {
	this.writerError = errors.New("")

	connection, connectError := this.connector.Connect(this.originalContext)
	_, err := connection.Writer(this.originalContext)

	this.So(connectError, should.BeNil)
	this.So(err, should.Equal, this.writerError)
	this.So(this.writerContext, should.Equal, this.originalContext)
}
func (this *ConnectorFixture) TestWhenOpeningCommitWriterFails_ReturnUnderlyingError() {
	this.commitWriterError = errors.New("")

	connection, connectError := this.connector.Connect(this.originalContext)
	_, err := connection.CommitWriter(this.originalContext)

	this.So(connectError, should.BeNil)
	this.So(err, should.Equal, this.commitWriterError)
	this.So(this.commitWriterContext, should.Equal, this.originalContext)
}

func (this *ConnectorFixture) TestClosingConnector_UnderlyingCloseCalled() {
	this.closeError = errors.New("")

	err := this.connector.Close()

	this.So(err, should.Equal, this.closeError)
}
func (this *ConnectorFixture) TestClosingConnection_UnderlyingCloseCalled() {
	this.closeError = errors.New("")

	connection, _ := this.connector.Connect(this.originalContext)
	err := connection.Close()

	this.So(err, should.Equal, this.closeError)
}
func (this *ConnectorFixture) TestClosingReader_UnderlyingCloseCalled() {
	this.closeError = errors.New("")
	connection, _ := this.connector.Connect(this.originalContext)
	reader, _ := connection.Reader(this.originalContext)

	err := reader.Close()

	this.So(err, should.Equal, this.closeError)
}
func (this *ConnectorFixture) TestClosingStream_UnderlyingCloseCalled() {
	this.closeError = errors.New("")
	connection, _ := this.connector.Connect(this.originalContext)
	reader, _ := connection.Reader(this.originalContext)
	stream, _ := reader.Stream(this.originalContext, messaging.StreamConfig{})

	err := stream.Close()

	this.So(err, should.Equal, this.closeError)
}
func (this *ConnectorFixture) TestClosingWriter_UnderlyingCloseCalled() {
	this.closeError = errors.New("")
	connection, _ := this.connector.Connect(this.originalContext)
	writer, _ := connection.Writer(this.originalContext)

	err := writer.Close()

	this.So(err, should.Equal, this.closeError)
}
func (this *ConnectorFixture) TestClosingCommitWriter_UnderlyingCloseCalled() {
	this.closeError = errors.New("")
	connection, _ := this.connector.Connect(this.originalContext)
	writer, _ := connection.CommitWriter(this.originalContext)

	err := writer.Close()

	this.So(err, should.Equal, this.closeError)
}
func (this *ConnectorFixture) TestCommittingCommitWriter_UnderlyingCommitCalled() {
	this.commitError = errors.New("")
	connection, _ := this.connector.Connect(this.originalContext)
	writer, _ := connection.CommitWriter(this.originalContext)

	err := writer.Commit()

	this.So(err, should.Equal, this.commitError)
}
func (this *ConnectorFixture) TestRollingBackCommitWriter_UnderlyingRollbackCalled() {
	this.rollbackError = errors.New("")
	this.commitError = errors.New("")
	connection, _ := this.connector.Connect(this.originalContext)
	writer, _ := connection.CommitWriter(this.originalContext)

	err := writer.Rollback()

	this.So(err, should.Equal, this.rollbackError)
}

func (this *ConnectorFixture) TestWhenOpeningStreamFails_ReturnUnderlyingError() {
	this.streamError = errors.New("")
	connection, _ := this.connector.Connect(this.originalContext)
	reader, _ := connection.Reader(this.originalContext)
	config := messaging.StreamConfig{StreamName: "queue"}

	_, err := reader.Stream(this.originalContext, config)

	this.So(err, should.Equal, this.streamError)
	this.So(this.streamContext, should.Equal, this.originalContext)
	this.So(this.streamConfig, should.Resemble, config)
}
func (this *ConnectorFixture) TestWhenReadingFromStreamFails_ReturnUnderlyingError() {
	this.streamReadError = errors.New("")
	connection, _ := this.connector.Connect(this.originalContext)
	reader, _ := connection.Reader(this.originalContext)
	stream, _ := reader.Stream(this.originalContext, messaging.StreamConfig{})
	var delivery messaging.Delivery

	err := stream.Read(this.originalContext, &delivery)

	this.So(err, should.Equal, this.streamReadError)
	this.So(this.streamReadContext, should.Equal, this.originalContext)
	this.So(this.streamReadDelivery, should.Equal, &delivery)
}
func (this *ConnectorFixture) TestWhenAcknowledgingDeliveryFails_ReturnUnderlyingError() {
	this.streamAckError = errors.New("")
	connection, _ := this.connector.Connect(this.originalContext)
	reader, _ := connection.Reader(this.originalContext)
	stream, _ := reader.Stream(this.originalContext, messaging.StreamConfig{})
	var delivery messaging.Delivery

	err := stream.Acknowledge(this.originalContext, delivery)

	this.So(err, should.Equal, this.streamAckError)
	this.So(this.streamAckContext, should.Equal, this.originalContext)
	this.So(this.streamAckDeliveries, should.Resemble, []messaging.Delivery{delivery})
}
func (this *ConnectorFixture) TestWhenReadingFromStream_DecodeDelivery() {
	this.decodeError = errors.New("")
	connection, _ := this.connector.Connect(this.originalContext)
	reader, _ := connection.Reader(this.originalContext)
	stream, _ := reader.Stream(this.originalContext, messaging.StreamConfig{})
	var delivery messaging.Delivery

	err := stream.Read(this.originalContext, &delivery)

	this.So(err, should.Equal, this.decodeError)
	this.So(delivery.MessageID, should.Equal, 42)
}
func (this *ConnectorFixture) TestWhenReadingFromStream_DecodeDeliveryOfDisallowedType_NoError() {
	this.decodeError = ErrMessageTypeNotAllowed
	connection, _ := this.connector.Connect(this.originalContext)
	reader, _ := connection.Reader(this.originalContext)
	stream, _ := reader.Stream(this.originalContext, messaging.StreamConfig{})
	var delivery messaging.Delivery

	err := stream.Read(this.originalContext, &delivery)

	this.So(err, should.BeNil)
	this.So(delivery.MessageID, should.Equal, 42)
}

func (this *ConnectorFixture) TestWhenWriting_EncodeThenCallInnerAndReturnUnderlyingError() {
	this.writeError = errors.New("")
	connection, _ := this.connector.Connect(this.originalContext)
	writer, _ := connection.Writer(this.originalContext)
	var dispatch messaging.Dispatch

	count, err := writer.Write(this.originalContext, dispatch)

	this.So(count, should.Equal, 1)
	this.So(err, should.Equal, this.writeError)
	this.So(this.writeDispatches, should.Resemble, []messaging.Dispatch{{MessageID: 42}})
	this.So(this.writeContext, should.Equal, this.originalContext)
}
func (this *ConnectorFixture) TestWhenCommitWriting_EncodeThenCallInnerAndReturnUnderlyingError() {
	this.writeError = errors.New("")
	connection, _ := this.connector.Connect(this.originalContext)
	writer, _ := connection.CommitWriter(this.originalContext)
	var dispatch messaging.Dispatch

	count, err := writer.Write(this.originalContext, dispatch)

	this.So(count, should.Equal, 1)
	this.So(err, should.Equal, this.writeError)
	this.So(this.writeDispatches, should.Resemble, []messaging.Dispatch{{MessageID: 42}})
	this.So(this.writeContext, should.Equal, this.originalContext)
}
func (this *ConnectorFixture) TestWhenWritingAndEncodeFails_ReturnUnderlyingError() {
	this.encodeError = errors.New("")
	connection, _ := this.connector.Connect(this.originalContext)
	writer, _ := connection.CommitWriter(this.originalContext)
	var dispatch messaging.Dispatch

	count, err := writer.Write(this.originalContext, dispatch)

	this.So(count, should.Equal, -1)
	this.So(err, should.Equal, this.encodeError)
	this.So(this.writeDispatches, should.BeEmpty)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *ConnectorFixture) Connect(ctx context.Context) (messaging.Connection, error) {
	this.connectContext = ctx
	return this, this.connectError
}
func (this *ConnectorFixture) Close() error {
	return this.closeError
}

func (this *ConnectorFixture) Reader(ctx context.Context) (messaging.Reader, error) {
	this.readerContext = ctx
	return this, this.readerError
}
func (this *ConnectorFixture) Writer(ctx context.Context) (messaging.Writer, error) {
	this.writerContext = ctx
	return this, this.writerError
}
func (this *ConnectorFixture) CommitWriter(ctx context.Context) (messaging.CommitWriter, error) {
	this.commitWriterContext = ctx
	return this, this.commitWriterError
}

func (this *ConnectorFixture) Write(ctx context.Context, dispatches ...messaging.Dispatch) (int, error) {
	this.writeContext = ctx
	this.writeDispatches = dispatches
	return len(dispatches), this.writeError
}
func (this *ConnectorFixture) Commit() error {
	return this.commitError
}
func (this *ConnectorFixture) Rollback() error {
	return this.rollbackError
}

func (this *ConnectorFixture) Stream(ctx context.Context, config messaging.StreamConfig) (messaging.Stream, error) {
	this.streamContext = ctx
	this.streamConfig = config
	return this, this.streamError
}
func (this *ConnectorFixture) Read(ctx context.Context, delivery *messaging.Delivery) error {
	this.streamReadContext = ctx
	this.streamReadDelivery = delivery
	return this.streamReadError
}
func (this *ConnectorFixture) Acknowledge(ctx context.Context, deliveries ...messaging.Delivery) error {
	this.streamAckContext = ctx
	this.streamAckDeliveries = deliveries
	return this.streamAckError
}

func (this *ConnectorFixture) Encode(dispatch *messaging.Dispatch) error {
	dispatch.MessageID = 42
	return this.encodeError
}
func (this *ConnectorFixture) Decode(delivery *messaging.Delivery) error {
	delivery.MessageID = 42
	return this.decodeError
}
