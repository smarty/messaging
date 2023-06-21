package batch

import (
	"context"

	"github.com/smarty/messaging/v4"
)

type Writer struct {
	connector messaging.Connector

	connection messaging.Connection
	writer     messaging.CommitWriter
}

func NewWriter(connector messaging.Connector) messaging.Writer {
	return &Writer{connector: connector}
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
	}

	return count, err

}
func (this *Writer) write(ctx context.Context, dispatches []messaging.Dispatch) (int, error) {
	if err := this.newWriter(ctx); err != nil {
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
func (this *Writer) newWriter(ctx context.Context) (err error) {
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
	if this.writer != nil {
		_ = this.writer.Close()
	}
	if this.connection != nil {
		_ = this.connection.Close()
	}

	this.writer = nil
	this.connection = nil
}
