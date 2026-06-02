package sqladapter

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/smarty/messaging/v3/handlers/harness"
)

type Writer struct {
	handle      *sql.DB
	typeNames   map[reflect.Type]string
	stride      uint64
	logger      Logger
	legacyWrite func(context.Context, *sql.Tx, ...any)
}

// NewWriter builds a Writer that inserts rows into the `Messages` table and
// invokes the supplied legacyWrite function inside the same transaction.
//
// Deprecation warning: the legacyWrite escape hatch is retained for migration from
// other projects and will be removed in a later release; new callers
// should supply a no-op function.
func NewWriter(handle *sql.DB, typeNames map[reflect.Type]string, stride uint64, logger Logger, legacyWrite func(context.Context, *sql.Tx, ...any)) *Writer {
	return &Writer{
		handle:      handle,
		typeNames:   typeNames,
		stride:      cmp.Or(stride, 1),
		logger:      logger,
		legacyWrite: legacyWrite,
	}
}

func (this *Writer) Write(ctx context.Context, messages ...any) (err error) {
	if len(messages) == 0 {
		return nil
	}

	tx, err := this.handle.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = tx.Rollback()
			err = fmt.Errorf("panic during write: %v", recovered)
		} else if err != nil {
			_ = tx.Rollback()
		}
	}()

	if err := this.insertMessages(ctx, tx, messages); err != nil {
		return err
	}

	this.legacyWrite(ctx, tx, messages...)

	return tx.Commit()
}

func (this *Writer) insertMessages(ctx context.Context, tx *sql.Tx, messages []any) error {
	var statement strings.Builder // TODO: reuse statement builder
	statement.WriteString(`INSERT INTO Messages (type, payload) VALUES `)
	args := make([]any, 0, len(messages)*2) // TODO: reuse slice/buffer
	for i, raw := range messages {
		message := raw.(*harness.Message)
		if message.Type == "" {
			message.Type = this.typeNames[reflect.TypeOf(message.Value)]
		}
		if i > 0 {
			statement.WriteString(`,`)
		}
		statement.WriteString(`(?, ?)`)
		args = append(args, message.Type, message.Content.Bytes())
	}
	result, err := tx.ExecContext(ctx, statement.String(), args...)
	if err != nil {
		return err
	}
	// https://dev.mysql.com/doc/refman/5.6/en/information-functions.html#function_last-insert-id
	// > If you insert multiple rows using a single INSERT statement, LAST_INSERT_ID() returns the value
	// > generated for the first inserted row only.
	// This Writer is the sole writer to the Messages table and processes each batch sequentially
	// inside a single transaction, so no other INSERT can interleave and create gaps between the
	// IDs assigned to this batch. The stride-based assignment below is therefore safe.
	first, err := result.LastInsertId()
	if err != nil {
		return err
	}
	for i, raw := range messages {
		raw.(*harness.Message).ID = uint64(first) + uint64(i)*this.stride
	}
	return nil
}
