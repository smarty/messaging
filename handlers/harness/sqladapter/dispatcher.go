// Package sqladapter provides a reference implementation of the
// handlers/harness Writer and Dispatcher interfaces, bound to the `Messages`
// MySQL table defined by doc/mysql/schema.sql in this module (columns
// `id`, `dispatched`, `type`, `payload`). Callers running a different schema
// should copy and adapt these types.
package sqladapter

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/smarty/messaging/v3"
	"github.com/smarty/messaging/v3/handlers/harness"
)

type Dispatcher struct {
	connector messaging.Connector
	handle    *sql.DB
	logger    Logger
}

func NewDispatcher(connector messaging.Connector, handle *sql.DB, logger Logger) *Dispatcher {
	return &Dispatcher{
		connector: connector,
		handle:    handle,
		logger:    logger,
	}
}

func (this *Dispatcher) Dispatch(ctx context.Context, messages ...*harness.Message) error {
	if len(messages) == 0 {
		return nil
	}
	if err := this.publish(ctx, messages); err != nil {
		return err
	}
	return this.markDispatched(ctx, messages)
}

func (this *Dispatcher) publish(ctx context.Context, messages []*harness.Message) error {
	connection, err := this.connector.Connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = connection.Close() }()

	writer, err := connection.Writer(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = writer.Close() }()

	dispatches := make([]messaging.Dispatch, 0, len(messages)) // TODO: reuse slice, pool dispatch struct
	for _, message := range messages {
		dispatches = append(dispatches, messaging.Dispatch{
			Durable:     true,
			MessageType: message.Type,
			ContentType: message.ContentType,
			Payload:     message.Content.Bytes(),
			Topic:       message.Type, // When payload is populated, the connector's encoder skips setting the topic.
		})
	}
	_, err = writer.Write(ctx, dispatches...)
	return err
}

func (this *Dispatcher) markDispatched(ctx context.Context, messages []*harness.Message) error {
	var statement strings.Builder
	statement.WriteString(`UPDATE Messages SET dispatched = NOW(3) WHERE id IN (`)
	args := make([]any, 0, len(messages))
	for i, message := range messages {
		if i > 0 {
			statement.WriteString(`,`)
		}
		statement.WriteString(`?`)
		args = append(args, message.ID)
	}
	statement.WriteString(`)`)
	if _, err := this.handle.ExecContext(ctx, statement.String(), args...); err != nil {
		return fmt.Errorf("mark dispatched: %w", err)
	}
	return nil
}
