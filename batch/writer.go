package batch

import (
	"context"
	"io"

	"github.com/smartystreets/messaging/v3"
)

type Writer struct {
	connector messaging.Connector

	connection          messaging.Connection
	writer              messaging.CommitWriter
	reuseWriteResources bool
	closeConnector      bool
}

func newWriter(config configuration) messaging.Writer {
	return &Writer{connector: config.Connector, reuseWriteResources: config.ReuseWriter, closeConnector: config.CloseConnector}
}

func (this *Writer) Write(ctx context.Context, dispatches ...messaging.Dispatch) (int, error) {
	if len(dispatches) == 0 {
		return 0, nil
	}

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	count, err := this.write(ctx, dispatches)
	if err != nil {
		this.closeHandles()
	} else if !this.reuseWriteResources {
		this.closeHandles()
	}

	return count, err
}
func (this *Writer) write(ctx context.Context, dispatches []messaging.Dispatch) (int, error) {
	if err := this.ensureWriter(ctx); err != nil {
		return 0, err
	}

	if _, err := this.writer.Write(ctx, dispatches...); err != nil {
		return 0, err
	}

	if err := this.writer.Commit(); err != nil {
		return 0, err
	}

	return len(dispatches), nil
}
func (this *Writer) ensureWriter(ctx context.Context) (err error) {
	if this.writer != nil {
		return nil
	}

	if this.connection, err = this.connector.Connect(ctx); err != nil {
		return err
	}

	this.writer, err = this.connection.CommitWriter(ctx)
	return err
}

func (this *Writer) Close() error {
	this.closeHandles()
	return nil
}
func (this *Writer) closeHandles() {
	closeResource(this.writer)
	this.writer = nil

	closeResource(this.connection)
	this.connection = nil

	if this.closeConnector {
		closeResource(this.connector)
		this.connector = nil
	}
}
func closeResource(resource io.Closer) {
	if resource != nil {
		_ = resource.Close()
	}
}
