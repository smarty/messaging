package harness

import (
	"context"
	"time"
)

// wait sleeps for d, or returns early with ctx.Err() if ctx is cancelled first.
func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
