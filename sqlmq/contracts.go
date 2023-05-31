package sqlmq

import (
	"context"
	"database/sql"

	"github.com/smartystreets/messaging/v3"
	"github.com/smartystreets/messaging/v3/sqlmq/adapter"
)

type messageStore interface {
	Store(ctx context.Context, writer adapter.Writer, dispatches []messaging.Dispatch) error
	Load(ctx context.Context, id uint64) ([]messaging.Dispatch, error)
	Confirm(ctx context.Context, dispatches []messaging.Dispatch) error
}

type transactionalContext interface {
	context.Context
	Store(tx *sql.Tx) // used by transactional handler
}

type monitor interface {
	MessageReceived(count int)
	MessageStored(count int)
	MessagePublished(count int)
	MessageConfirmed(count int)
}

type logger interface {
	Printf(format string, args ...any)
}
