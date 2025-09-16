package retry

import (
	"context"
	"math/rand/v2"
	"runtime/debug"
	"time"

	"github.com/smarty/messaging/v3"
)

type handler struct {
	messaging.Handler
	minBackoff   time.Duration
	maxBackoff   time.Duration
	jitterFactor float64
	maxAttempts  int
	logger       logger
	monitor      monitor
	stackTrace   bool
	immediate    map[any]struct{}
}

func (this handler) Handle(ctx context.Context, messages ...any) {
	for attempt := 0; isAlive(ctx); attempt++ {
		if this.handle(ctx, attempt, messages...) {
			break
		}
	}
}
func (this handler) handle(ctx context.Context, attempt int, messages ...any) (success bool) {
	defer func() { success = this.finally(ctx, attempt, recover()) }()
	this.Handler.Handle(ctx, messages...)
	return success
}
func (this handler) finally(ctx context.Context, attempt int, err any) bool {
	this.monitor.HandleAttempted(attempt, err)

	if err != nil {
		this.handleFailure(ctx, attempt, err)
	} else if attempt > 0 {
		this.logger.Printf("[INFO] Operation completed successfully after [%d] failed attempt(s).", attempt)
	}

	return err == nil
}

func (this handler) handleFailure(ctx context.Context, attempt int, err any) {
	this.logFailure(attempt, err)
	this.panicOnTooManyAttempts(attempt)
	this.sleep(ctx, attempt, err)
}
func (this handler) logFailure(attempt int, err any) {
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
func (this handler) sleep(ctx context.Context, attempt int, err any) {
	if _, contains := this.immediate[err]; contains {
		return
	}
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, this.backoffDelay(attempt))
	defer timeoutCancel()
	<-timeoutCtx.Done()
}

func (this handler) backoffDelay(attempt int) time.Duration {
	if attempt == 0 || this.maxBackoff == 0 {
		return this.minBackoff
	}

	backoff := this.minBackoff << min(attempt, 63)
	delay := min(backoff, this.maxBackoff)

	if this.jitterFactor > 0 && this.jitterFactor <= 1.0 {
		jitterRange := float64(delay) * this.jitterFactor
		minDelay := float64(delay) - jitterRange
		delay = time.Duration(minDelay + rand.Float64()*(2*jitterRange))
	}

	return delay
}

func isAlive(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	default:
		return true
	}
}
