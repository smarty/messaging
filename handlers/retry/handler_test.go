package retry

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/messaging/v4"
)

func TestFixture(t *testing.T) {
	gunit.Run(new(Fixture), t)
}

type Fixture struct {
	*gunit.Fixture

	handler messaging.Handler

	expectedContext       context.Context
	shutdown              context.CancelFunc
	expectedMessages      []interface{}
	receivedContext       context.Context
	receivedMessages      []interface{}
	handleError           error
	handleCalls           int
	noErrorAfterAttempt   int
	shutdownAfterAttempt  int
	loggedMessages        []string
	monitoredAttemptCount int
	monitoredErrors       []interface{}
}

func (this *Fixture) Setup() {
	this.expectedContext = context.WithValue(context.Background(), reflect.TypeOf(this), this)
	this.expectedContext, this.shutdown = context.WithCancel(this.expectedContext)
	this.expectedMessages = []interface{}{1, 2, 3}
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
	this.So(this.receivedMessages, should.Resemble, this.expectedMessages)
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
	this.So(this.monitoredErrors, should.Resemble, []interface{}{this.handleError, nil})
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
	this.So(this.monitoredErrors, should.Resemble, []interface{}{this.handleError, this.handleError})
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *Fixture) Handle(ctx context.Context, messages ...interface{}) {
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

func (this *Fixture) Printf(format string, args ...interface{}) {
	this.loggedMessages = append(this.loggedMessages, fmt.Sprintf(format, args...))
}

func (this *Fixture) HandleAttempted(attempt int, err interface{}) {
	this.monitoredAttemptCount = attempt
	this.monitoredErrors = append(this.monitoredErrors, err)
}
