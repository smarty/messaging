package retry

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/smarty/gunit"
	"github.com/smarty/gunit/assert/should"
	"github.com/smarty/messaging/v4"
)

func TestFixture(t *testing.T) {
	gunit.Run(new(Fixture), t)
}

type Fixture struct {
	*gunit.Fixture

	handler messaging.Handler

	expectedContext       context.Context
	shutdown              context.CancelFunc
	expectedMessages      []any
	receivedContext       context.Context
	receivedMessages      []any
	handleError           error
	handleCalls           int
	noErrorAfterAttempt   int
	shutdownAfterAttempt  int
	loggedMessages        []string
	monitoredAttemptCount int
	monitoredErrors       []any
}

func (this *Fixture) Setup() {
	this.expectedContext = context.WithValue(context.Background(), reflect.TypeOf(this), this)
	this.expectedContext, this.shutdown = context.WithCancel(this.expectedContext)
	this.expectedMessages = []any{1, 2, 3}
	this.handler = New(this,
		Options.Logger(this),
		Options.Monitor(this),
	)
}
func (this *Fixture) Teardown() {
	this.shutdown()
}

func (this *Fixture) handle() {
	this.handler.Handle(this.expectedContext, this.expectedMessages...)
}
func (this *Fixture) assertCallToInnerHandler() {
	this.So(this.receivedMessages, should.Equal, this.expectedMessages)
	this.So(this.receivedContext.Value(reflect.TypeOf(this)), should.Equal, this)
}

func (this *Fixture) TestInnerHandlerCalledProperly() {
	this.handle()

	this.assertCallToInnerHandler()
}
func (this *Fixture) TestCancelledContextDoesNotCallInnerHandler() {
	this.shutdown()

	this.handle()

	this.So(this.receivedContext, should.BeNil)
	this.So(this.receivedMessages, should.BeNil)
}
func (this *Fixture) SkipTestSleepBetweenRetries() {
	this.handleError = errors.New("failed")
	this.noErrorAfterAttempt = 2
	this.handler = New(this,
		Options.MaxAttempts(1),
		Options.Timeout(time.Millisecond),
		Options.Monitor(this),
		Options.Logger(this),
	)

	start := time.Now().UTC()
	this.handle()

	this.So(start, should.HappenWithin, time.Millisecond*5, time.Now().UTC())
	this.So(this.monitoredAttemptCount, should.Equal, 1)
	this.So(this.monitoredErrors, should.Equal, []any{this.handleError, nil})
	this.So(this.loggedMessages[0], should.ContainSubstring, "debug.Stack")
}
func (this *Fixture) TestPanicOnTooManyFailedAttempts() {
	this.handleError = errors.New("failed")
	this.handler = New(this,
		Options.MaxAttempts(1),
		Options.Timeout(time.Millisecond),
		Options.Monitor(this),
		Options.Logger(this),
	)

	this.So(this.handle, should.PanicWith, ErrMaxRetriesExceeded)

	this.So(this.handleCalls, should.Equal, 2)
	this.So(this.monitoredAttemptCount, should.Equal, 1)
	this.So(this.monitoredErrors, should.Equal, []any{this.handleError, this.handleError})
}
func (this *Fixture) TestNoMoreRetriesOnCancelledContext() {
	this.handleError = errors.New("failed")
	this.shutdownAfterAttempt = 1

	this.So(this.handle, should.NotPanic)

	this.So(this.handleCalls, should.Equal, 1)
}

func (this *Fixture) TestDontLogStackTrace() {
	this.handleError = errors.New("failed")
	this.noErrorAfterAttempt = 2
	this.handler = New(this,
		Options.Timeout(time.Millisecond),
		Options.LogStackTrace(false),
		Options.Logger(this),
	)

	this.So(this.handle, should.NotPanic)

	this.So(this.loggedMessages[0], should.NotContainSubstring, "debug.Stack")
}

func (this *Fixture) TestWhenRecoveryGivesSpecifiedError_DoNotSleepAndRetryImmediately() {
	this.handleError = errors.New("")
	this.handler = New(this,
		Options.Logger(this),
		Options.Monitor(this),
		Options.MaxAttempts(3),
		Options.Timeout(time.Millisecond*100),
		Options.ImmediateRetry(this.handleError),
	)

	started := time.Now().UTC()
	this.So(this.handle, should.PanicWith, ErrMaxRetriesExceeded)
	this.So(time.Since(started), should.BeLessThan, time.Millisecond*10)
}

func (this *Fixture) TestBackoffDelay_NeverOverflowsOrDropsToZero() {
	subject := New(this,
		Options.Backoff(time.Second*5),
		Options.MaxBackoff(time.Minute*5),
		Options.JitterFactor(0.0),
	).(handler)

	this.So(subject.backoffDelay(0), should.Equal, time.Second*5)
	this.So(subject.backoffDelay(1), should.Equal, time.Second*10)
	this.So(subject.backoffDelay(6), should.Equal, time.Minute*5) // 5s << 6 = 320s, capped
	for _, attempt := range []int{31, 32, 62, 63, 64, 200, 1 << 20} {
		this.So(subject.backoffDelay(attempt), should.Equal, time.Minute*5)
	}
}
func (this *Fixture) LongTestExponentialBackoff() {
	this.handleError = errors.New("failed")
	this.noErrorAfterAttempt = 4
	this.handler = New(this,
		Options.MaxAttempts(3),
		Options.Backoff(time.Millisecond*50),
		Options.MaxBackoff(time.Millisecond*500),
		Options.JitterFactor(0.0),
		Options.Monitor(this),
		Options.Logger(this),
	)

	start := time.Now().UTC()
	this.So(this.handle, should.NotPanic)
	elapsed := time.Since(start)

	expected := 50*time.Millisecond + 100*time.Millisecond + 200*time.Millisecond
	this.So(elapsed, should.BeBetween, expected, expected+5*time.Millisecond)
	this.So(this.monitoredAttemptCount, should.Equal, 3)
}
func (this *Fixture) LongTestBackoffMaxTimeoutNotExceeded() {
	this.handleError = errors.New("failed")
	this.noErrorAfterAttempt = 5
	this.handler = New(this,
		Options.MaxAttempts(4),
		Options.Backoff(time.Millisecond*50),
		Options.MaxBackoff(time.Millisecond*100),
		Options.JitterFactor(0.0),
		Options.Monitor(this),
		Options.Logger(this),
	)

	start := time.Now().UTC()
	this.handle()
	elapsed := time.Since(start)

	expected := 50*time.Millisecond + 100*time.Millisecond*3
	this.So(elapsed, should.BeBetween, expected, expected+5*time.Millisecond)
	this.So(this.monitoredAttemptCount, should.Equal, 4)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *Fixture) Handle(ctx context.Context, messages ...any) {
	this.handleCalls++
	this.receivedContext = ctx
	this.receivedMessages = append(this.receivedMessages, messages...)

	if this.shutdownAfterAttempt > 0 && this.handleCalls >= this.shutdownAfterAttempt {
		this.shutdown()
	}

	if this.noErrorAfterAttempt > 0 && this.handleCalls >= this.noErrorAfterAttempt {
		return
	}

	if this.handleError != nil {
		panic(this.handleError)
	}
}

func (this *Fixture) Printf(format string, args ...any) {
	this.loggedMessages = append(this.loggedMessages, fmt.Sprintf(format, args...))
}

func (this *Fixture) HandleAttempted(attempt int, err any) {
	this.monitoredAttemptCount = attempt
	this.monitoredErrors = append(this.monitoredErrors, err)
}
