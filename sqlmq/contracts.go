package sqlmq

import (
	"context"
	"database/sql"

	"github.com/smarty/messaging/v4"
	"github.com/smarty/messaging/v4/sqlmq/adapter"
)

type messageStore interface {
	Store(ctx context.Context, writer adapter.Writer, dispatches []messaging.Dispatch) error
	Load(ctx context.Context, id uint64) ([]messaging.Dispatch, error)
	Confirm(ctx context.Context, dispatches []messaging.Dispatch) (confirmed int, err error)
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

// StartupRecoveryMonitor is an optional extension of the monitor given to
// Options.Monitor. When that monitor also implements it, the dispatch processor
// reports what startup recovery found. It is exported so implementers can assert
// at compile time that their methods match; a mismatch would otherwise be silent.
type StartupRecoveryMonitor interface {
	// StartupRecovered fires after each startup load that finds undispatched
	// messages, with the number found. A load that fails part way is retried
	// from the last message it returned, so the counts of every call add up to
	// the total recovered.
	StartupRecovered(messageCount int)
}

// HandoffMonitor is an optional extension of the monitor given to
// Options.Monitor. When that monitor also implements it, each committed outbox
// transaction reports a handoff to the dispatch processor that did not finish
// within HandoffTimeout. It is exported for the same reason as
// StartupRecoveryMonitor.
type HandoffMonitor interface {
	// HandoffDeferred fires when the dispatch processor did not accept every
	// committed message within HandoffTimeout, with the number moved to a
	// background handoff.
	HandoffDeferred(messageCount int)
	// HandoffCapacityReached fires when a deferral would exceed
	// DeferredHandoffCapacity, so Commit waits for the dispatch processor
	// instead and the caller feels the back-pressure.
	HandoffCapacityReached()
}

type logger interface {
	Printf(format string, args ...any)
}
