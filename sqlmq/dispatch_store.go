package sqlmq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/smartystreets/messaging/v4"
	"github.com/smartystreets/messaging/v4/sqlmq/adapter"
)

type dispatchStore struct {
	db               adapter.ReadWriter
	now              func() time.Time
	confirmStatement *strings.Builder
}

func newMessageStore(db adapter.ReadWriter, now func() time.Time) messageStore {
	return dispatchStore{db: db, now: now, confirmStatement: &strings.Builder{}}
}

func (this dispatchStore) Store(ctx context.Context, writer adapter.Writer, dispatches []messaging.Dispatch) error {
	length := uint64(len(dispatches))
	if length == 0 {
		return nil
	}

	statement, args := this.buildExecArgs(dispatches)
	result, err := writer.ExecContext(ctx, statement, args...)
	if err != nil {
		return err
	}

	affected, _ := result.RowsAffected()
	if affected != int64(length) {
		return errRowsAffected
	}

	identity, _ := result.LastInsertId()
	if identity <= 0 {
		return errIdentityFailure
	}

	for i := uint64(0); i < length; i++ {
		dispatches[i].MessageID = uint64(identity) + i
	}

	return nil
}
func (this dispatchStore) buildExecArgs(dispatches []messaging.Dispatch) (string, []interface{}) {
	builder := &strings.Builder{}
	args := make([]interface{}, 0, len(dispatches)*2)

	_, _ = builder.WriteString("INSERT INTO Messages (type, payload) VALUES ")
	for i, dispatch := range dispatches {
		args = append(args, dispatch.MessageType, dispatch.Payload)
		if i == len(dispatches)-1 {
			_, _ = builder.WriteString("(?,?);")
		} else {
			_, _ = builder.WriteString("(?,?),")
		}
	}

	return builder.String(), args
}

func (this dispatchStore) Load(ctx context.Context, id uint64) (results []messaging.Dispatch, err error) {
	statement := fmt.Sprintf("SELECT id, type, payload FROM Messages WHERE dispatched IS NULL AND id > %d;", id)
	rows, err := this.db.QueryContext(ctx, statement)
	if err != nil {
		return nil, err
	}
	defer closeResource(rows)

	now := this.now().UTC()
	for rows.Next() {
		dispatch := messaging.Dispatch{Timestamp: now}
		if err := rows.Scan(&dispatch.MessageID, &dispatch.MessageType, &dispatch.Payload); err != nil {
			return nil, err
		}

		dispatch.Durable = true
		dispatch.ContentType = "application/json"
		dispatch.Topic = dispatch.MessageType
		results = append(results, dispatch)
	}

	return results, rows.Err()
}
func (this dispatchStore) Confirm(ctx context.Context, dispatches []messaging.Dispatch) error {
	if len(dispatches) == 0 {
		return nil
	}

	defer this.confirmStatement.Reset()

	for i, dispatch := range dispatches {
		var template = "%d, "
		if i+1 >= len(dispatches) {
			template = "%d"
		}
		_, _ = fmt.Fprintf(this.confirmStatement, template, dispatch.MessageID)
	}

	now := this.now().UTC().Format("2006-01-02 15:04:05.000000")
	const statementFormat = "UPDATE Messages SET dispatched = '%s' WHERE dispatched IS NULL AND id IN (%s);"
	statement := fmt.Sprintf(statementFormat, now, this.confirmStatement.String())
	_, err := this.db.ExecContext(ctx, statement)
	return err
}

func closeResource(resource io.Closer) {
	if resource != nil {
		_ = resource.Close()
	}
}

var (
	errRowsAffected    = errors.New("the number of modified rows was not expected compared to the number of writes performed")
	errIdentityFailure = errors.New("unable to determine the identity of the inserted row(s)")
)
