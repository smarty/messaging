package retry

import (
	"context"
	"runtime/debug"
	"time"

	"github.com/smartystreets/messaging/v3"
)

type handler struct {
	messaging.Handler
	timeout     time.Duration
	maxAttempts int
	logger      logger
	monitor     monitor
	stackTrace  bool
	immediate   map[interface{}]struct{}
}

func (this handler) Handle(ctx context.Context, messages ...interface{}) {
	for attempt := 0; isAlive(ctx); attempt++ {
		if this.handle(ctx, attempt, messages...) {
			break
		}
	}
}
func (this handler) handle(ctx context.Context, attempt int, messages ...interface{}) (success bool) {
	defer func() { success = this.finally(ctx, attempt, recover()) }()
	this.Handler.Handle(ctx, messages...)
	return success
}
func (this handler) finally(ctx context.Context, attempt int, err interface{}) bool {
	this.monitor.Attempt(attempt, err)

	if err != nil {
		this.handleFailure(ctx, attempt, err)
	} else if attempt > 0 {
		this.logger.Printf("[INFO] Operation completed successfully after [%d] failed attempt(s).", attempt)
	}

	return err == nil
}

func (this handler) handleFailure(ctx context.Context, attempt int, err interface{}) {
	this.logFailure(attempt, err)
	this.panicOnTooManyAttempts(attempt)
	this.sleep(ctx, err)
}
func (this handler) logFailure(attempt int, err interface{}) {
	if this.stackTrace {
		this.logger.Printf("[INFO] Attempt [%d] operation failure [%s].\n%s", attempt, err, string(debug.Stack()))
	} else {
		this.logger.Printf("[INFO] Attempt [%d] operation failure [%s].", attempt, err)
	}
}
func (this handler) panicOnTooManyAttempts(attempt int) {
	if this.maxAttempts > 0 && attempt >= this.maxAttempts {
		panic(ErrMaxRetriesExceeded)
	}
}
func (this handler) sleep(ctx context.Context, err interface{}) {
	if _, contains := this.immediate[err]; contains {
		return
	}
	ctx, _ = context.WithTimeout(ctx, this.timeout)
	<-ctx.Done()
}

func isAlive(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	default:
		return true
	}
}
