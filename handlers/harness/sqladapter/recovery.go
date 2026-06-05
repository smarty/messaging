package sqladapter

import (
	"bytes"
	"context"
	"database/sql"

	"github.com/smarty/messaging/v3/handlers/harness"
)

// Recover scans Messages WHERE dispatched IS NULL at startup, wraps each row
// as a *harness.Message, and feeds them through the Dispatcher (publish + mark dispatched)
// in pages of batchSize. Intended to run synchronously during initialization, before
// the harness pipeline starts.
//
// TODO: make this a Listener
// TODO: retry w/ backoff
func Recover(ctx context.Context, handle *sql.DB, dispatcher *Dispatcher, logger Logger, batchSize int) error {
	logger.Printf("[INFO] Recovering undispatched message(s) from previous run...")
	rows, err := handle.QueryContext(ctx, `
		SELECT id, type, payload
		  FROM Messages
		 WHERE dispatched IS NULL
		 ORDER BY id`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	var total int
	var messages []*harness.Message
	for rows.Next() {
		var (
			id       uint64
			typeName string
			payload  []byte
		)
		if err := rows.Scan(&id, &typeName, &payload); err != nil {
			return err
		}
		total++
		messages = append(messages, &harness.Message{
			ID:      id,
			Type:    typeName,
			Content: bytes.NewBuffer(payload),
			// Hard-coded until the Messages schema gains a content_type column;
			// all payloads written by Writer are JSON so this is correct for now.
			ContentType: "application/json",
		})
		if len(messages) >= batchSize {
			err := dispatcher.Dispatch(ctx, messages...)
			if err != nil {
				return err
			}
			clear(messages)
			messages = messages[:0]
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(messages) > 0 {
		err = dispatcher.Dispatch(ctx, messages...)
	}
	logger.Printf("[INFO] Recovering %d total undispatched message(s) from previous run.", total)
	return err
}
