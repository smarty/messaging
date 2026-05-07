package sqladapter

import (
	"bytes"
	"context"
	"database/sql"

	"github.com/smarty/messaging/v3/handlers/harness"
)

// Recover scans Messages WHERE dispatched IS NULL at startup, wraps each row
// as a *harness.Message, and feeds them through the Dispatcher (publish + mark dispatched).
// Intended to run synchronously during initialization, before the harness pipeline starts.
//
// TODO: pagination/streaming for large backlogs (currently loads everything into memory).
// TODO: make this a Listener
// TODO: retry w/ backoff
func Recover(ctx context.Context, handle *sql.DB, dispatcher *Dispatcher, logger Logger) error {
	rows, err := handle.QueryContext(ctx, `
		SELECT id, type, payload
		  FROM Messages
		 WHERE dispatched IS NULL
		 ORDER BY id`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	var messages []any
	for rows.Next() {
		var (
			id       uint64
			typeName string
			payload  []byte
		)
		if err := rows.Scan(&id, &typeName, &payload); err != nil {
			return err
		}
		messages = append(messages, &harness.Message{
			ID:          id,
			Type:        typeName,
			Content:     bytes.NewBuffer(payload),
			ContentType: "application/json",
		})
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(messages) == 0 {
		return nil
	}
	logger.Printf("[INFO] Recovering %d undispatched message(s) from previous run.", len(messages))
	return dispatcher.Dispatch(ctx, messages...)
}
