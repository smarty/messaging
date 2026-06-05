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

// Deprecated
type legacyWrite func(context.Context, *sql.Tx, ...*harness.Message)

type Writer struct {
	handle      *sql.DB
	typeNames   map[reflect.Type]string
	stride      uint64
	logger      Logger
	legacyWrite legacyWrite
}

// NewWriter builds a Writer that inserts rows into the `Messages` table and
// invokes the supplied legacyWrite function inside the same transaction.
//
// Deprecation warning: the legacyWrite escape hatch is retained for migration from
// other projects and will be removed in a later release; new callers
// should supply a no-op function.
func NewWriter(handle *sql.DB, typeNames map[reflect.Type]string, stride uint64, logger Logger, legacyWrite legacyWrite) *Writer {
	return &Writer{
		handle:      handle,
		typeNames:   typeNames,
		stride:      cmp.Or(stride, 1),
		logger:      logger,
		legacyWrite: legacyWrite,
	}
}

func (this *Writer) Write(ctx context.Context, messages ...*harness.Message) (err error) {
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

func (this *Writer) insertMessages(ctx context.Context, tx *sql.Tx, messages []*harness.Message) error {
	var statement strings.Builder // TODO: reuse statement builder
	statement.WriteString(`INSERT INTO Messages (type, payload) VALUES `)
	args := make([]any, 0, len(messages)*2) // TODO: reuse slice/buffer
	for i, message := range messages {
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
	first, err := result.LastInsertId()
	if err != nil {
		return err
	}
	for i, message := range messages {
		message.ID = uint64(first) + uint64(i)*this.stride
	}
	return nil
}
