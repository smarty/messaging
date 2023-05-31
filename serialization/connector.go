package serialization

import (
	"context"

	"github.com/smartystreets/messaging/v3"
)

type defaultConnector struct {
	messaging.Connector
	config configuration
}

func newConnector(config configuration) messaging.Connector {
	return defaultConnector{Connector: config.Connector, config: config}
}
func (this defaultConnector) Connect(ctx context.Context) (messaging.Connection, error) {
	if connection, err := this.Connector.Connect(ctx); err != nil {
		return nil, err
	} else {
		return newConnection(connection, this.config), nil
	}
}

type defaultConnection struct {
	messaging.Connection
	config configuration
}

func newConnection(inner messaging.Connection, config configuration) messaging.Connection {
	// NOTE: pointer receiver allows pointer equality when connection equality between two instances
	return &defaultConnection{Connection: inner, config: config}

}
func (this *defaultConnection) Reader(ctx context.Context) (messaging.Reader, error) {
	if reader, err := this.Connection.Reader(ctx); err != nil {
		return nil, err
	} else {
		return newReader(reader, this.config), nil
	}
}
func (this *defaultConnection) Writer(ctx context.Context) (messaging.Writer, error) {
	if writer, err := this.Connection.Writer(ctx); err != nil {
		return nil, err
	} else {
		return newWriter(writer.(messaging.CommitWriter), this.config), nil
	}
}
func (this *defaultConnection) CommitWriter(ctx context.Context) (messaging.CommitWriter, error) {
	if commitWriter, err := this.Connection.CommitWriter(ctx); err != nil {
		return nil, err
	} else {
		return newWriter(commitWriter, this.config), nil
	}
}

type defaultReader struct {
	messaging.Reader
	config configuration
}

func newReader(inner messaging.Reader, config configuration) messaging.Reader {
	return defaultReader{Reader: inner, config: config}
}
func (this defaultReader) Stream(ctx context.Context, config messaging.StreamConfig) (messaging.Stream, error) {
	if inner, err := this.Reader.Stream(ctx, config); err != nil {
		return nil, err
	} else {
		return newStream(inner, this.config), nil
	}
}

type defaultStream struct {
	messaging.Stream
	decoder DeliveryDecoder
	monitor monitor
}

func newStream(inner messaging.Stream, config configuration) messaging.Stream {
	return defaultStream{Stream: inner, decoder: config.Decoder, monitor: config.Monitor}
}
func (this defaultStream) Read(ctx context.Context, delivery *messaging.Delivery) error {
	if err := this.Stream.Read(ctx, delivery); err != nil {
		return err
	}

	if err := this.decoder.Decode(delivery); err == ErrMessageTypeNotAllowed {
		return nil
	} else {
		return err
	}
}

type defaultWriter struct {
	messaging.CommitWriter
	encoder DispatchEncoder
	monitor monitor
}

func newWriter(inner messaging.CommitWriter, config configuration) messaging.CommitWriter {
	return defaultWriter{CommitWriter: inner, encoder: config.Encoder, monitor: config.Monitor}
}
func (this defaultWriter) Write(ctx context.Context, dispatches ...messaging.Dispatch) (int, error) {
	for i, _ := range dispatches {
		if err := this.encoder.Encode(&dispatches[i]); err != nil {
			return -1, err
		}
	}

	return this.CommitWriter.Write(ctx, dispatches...)
}
