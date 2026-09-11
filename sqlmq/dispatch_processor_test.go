package sqlmq

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/smarty/gunit"
	"github.com/smarty/gunit/assert/should"
	"github.com/smarty/messaging/v4"
	"github.com/smarty/messaging/v4/sqlmq/adapter"
)

func TestDispatchProcessorFixture(t *testing.T) {
	gunit.Run(new(DispatchProcessorFixture), t)
}

type DispatchProcessorFixture struct {
	*gunit.Fixture

	ctx          context.Context
	channel      chan messaging.Dispatch
	sleepTimeout time.Duration
	listener     messaging.ListenCloser
	log          bytes.Buffer

	writeCount        int
	writeFailureUntil int
	writeBlocks       bool
	writeContext      context.Context
	writeDispatches   []messaging.Dispatch
	writeError        error

	confirmCount        int
	confirmFailureUntil int
	confirmContext      context.Context
	confirmDispatches   []messaging.Dispatch
	confirmError        error

	loadCount         int
	loadMaxResultsPer int
	loadContext       context.Context
	loadID            uint64
	loadResult        []messaging.Dispatch
	loadError         error

	closeCount int
}

func (this *DispatchProcessorFixture) Setup() {
	this.ctx = context.Background()
	this.sleepTimeout = time.Microsecond * 100
	this.channel = make(chan messaging.Dispatch, 4)
	this.initializeDispatchProcessor()
}
func (this *DispatchProcessorFixture) initializeDispatchProcessor() {
	_, this.listener = New(nil,
		Options.MessageStore(this),
		Options.MessageSender(this),
		Options.Context(this.ctx),
		Options.Channel(this.channel),
		Options.RetryTimeout(this.sleepTimeout),
		Options.StorageHandle(&sql.DB{}),
		Options.Logger(this),
	)
}
func (this *DispatchProcessorFixture) listen(sleep time.Duration) {
	go func() {
		time.Sleep(sleep)
		_ = this.listener.Close()
	}()

	this.listener.Listen() // blocks
}

func (this *DispatchProcessorFixture) TestWhenClose_CloseSenderAndAllowListenToExit() {
	this.listen(time.Millisecond)

	this.So(this.closeCount, should.Equal, 1)
}
func (this *DispatchProcessorFixture) TestWhenClose_ChannelStaysOpenForHandoffsStillInFlight() {
	this.listen(time.Millisecond)

	// A handler that committed SQL just before shutdown, or a deferred handoff
	// goroutine, may still be sending. A closed channel would panic them.
	this.So(func() { this.channel <- messaging.Dispatch{} }, should.NotPanic)
	this.So(len(this.channel), should.Equal, 1)
}
func (this *DispatchProcessorFixture) TestWhenDispatchesArePending_ItShouldPublishThemAndConfirmDispatch() {
	expected := []messaging.Dispatch{{MessageID: 1}, {MessageID: 2}, {MessageID: 3}}
	for _, item := range expected {
		this.channel <- item
	}

	this.listen(time.Millisecond)

	this.So(this.writeCount, should.Equal, 1)
	this.So(this.writeContext, should.NotBeNil)
	this.So(this.writeDispatches, should.Equal, expected)

	this.So(this.confirmCount, should.Equal, 1)
	this.So(this.confirmContext, should.NotBeNil)
	this.So(this.confirmDispatches, should.Equal, expected)
}
func (this *DispatchProcessorFixture) TestWhenWritingToSenderFails_TryAgain() {
	this.writeError = errors.New("")
	this.writeFailureUntil = 1 // 0th attempt fails, subsequent attempts pass

	expected := []messaging.Dispatch{{MessageID: 1}, {MessageID: 2}, {MessageID: 3}}
	for _, item := range expected {
		this.channel <- item
	}

	this.listen(time.Millisecond * 3)

	this.So(this.writeCount, should.Equal, 2)
	this.So(this.writeContext, should.NotBeNil)
	this.So(this.writeDispatches, should.Equal, append(expected, expected...))

	this.So(this.confirmCount, should.Equal, 1)
	this.So(this.confirmContext, should.NotBeNil)
	this.So(this.confirmDispatches, should.Equal, expected)
}
func (this *DispatchProcessorFixture) TestWhenConfirmDispatchFails_TryAgainWithoutWritingToSenderTwice() {
	this.confirmError = errors.New("")
	this.confirmFailureUntil = 1 // 0th attempt fails, subsequent attempts pass

	expected := []messaging.Dispatch{{MessageID: 1}, {MessageID: 2}, {MessageID: 3}}
	for _, item := range expected {
		this.channel <- item
	}

	this.listen(time.Millisecond * 10)

	this.So(this.writeCount, should.Equal, 1)
	this.So(this.writeContext, should.NotBeNil)
	this.So(this.writeDispatches, should.Equal, expected)

	this.So(this.confirmCount, should.Equal, 2)
	this.So(this.confirmContext, should.NotBeNil)
	this.So(this.confirmDispatches, should.Equal, append(expected, expected...))
}
func (this *DispatchProcessorFixture) TestWhenClosingDuringSleepRetry_SleepIsCutShort() {
	this.sleepTimeout = time.Minute
	this.initializeDispatchProcessor()

	this.confirmError = errors.New("") // sleep begins on error for 1 min
	this.channel <- messaging.Dispatch{}

	start := time.Now().UTC()
	this.listen(time.Millisecond * 3)
	completed := time.Since(start)

	this.So(completed, should.BeLessThan, time.Millisecond*10)
}

func (this *DispatchProcessorFixture) TestWhenClosingWhileStartupReadIsBlockedOnFullChannel_ListenStillReturns() {
	this.channel = make(chan messaging.Dispatch, 1)
	this.initializeDispatchProcessor()
	this.writeBlocks = true
	this.loadResult = []messaging.Dispatch{{MessageID: 1}, {MessageID: 2}, {MessageID: 3}, {MessageID: 4}, {MessageID: 5}}

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		this.listen(time.Millisecond * 5)
	}()

	select {
	case <-returned:
		this.So(true, should.BeTrue)
	case <-time.After(time.Millisecond * 50):
		this.So("Listen did not return within 50ms", should.BeBlank)
	}
}

func (this *DispatchProcessorFixture) TestWhenStartupReadFindsRows_LogTheCount() {
	this.loadResult = []messaging.Dispatch{{MessageID: 1}, {MessageID: 2}}

	this.listen(time.Millisecond * 5)

	this.So(this.log.String(), should.ContainSubstring, "[INFO] Startup recovery found [2] undispatched message(s) in durable storage.")
}
func (this *DispatchProcessorFixture) TestWhenStartupReadFindsNoRows_LogNothing() {
	this.listen(time.Millisecond * 5)

	this.So(this.log.String(), should.NotContainSubstring, "Startup recovery found")
}

func (this *DispatchProcessorFixture) TestWhenListening_ReadPendingDispatchesFromStorage() {
	expected := []messaging.Dispatch{{MessageID: 1}, {MessageID: 2}, {MessageID: 3}}
	this.loadResult = expected

	this.listen(time.Millisecond * 50)

	this.So(this.writeCount, should.Equal, 1)
	this.So(this.writeContext, should.NotBeNil)
	this.So(this.writeDispatches, should.Equal, expected)

	this.So(this.confirmCount, should.Equal, 1)
	this.So(this.confirmContext, should.NotBeNil)
	this.So(this.confirmDispatches, should.Equal, expected)
}
func (this *DispatchProcessorFixture) SkipTestWhenInitializeOperationDoesNotGetAllPendingDispatches_ReadFromLastSuccess() {
	this.loadResult = []messaging.Dispatch{{MessageID: 1}, {MessageID: 2}, {MessageID: 3}, {MessageID: 4}, {MessageID: 5}}
	this.loadMaxResultsPer = 2
	this.loadError = errors.New("")

	this.listen(time.Millisecond * 25)

	this.So(this.writeCount, should.Equal, 3)
	this.So(this.writeContext, should.NotBeNil)
	this.So(this.writeDispatches, should.Equal, this.loadResult)

	this.So(this.confirmCount, should.Equal, 3)
	this.So(this.confirmContext, should.NotBeNil)
	this.So(this.confirmDispatches, should.Equal, this.loadResult)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *DispatchProcessorFixture) Printf(format string, args ...any) {
	_, _ = fmt.Fprintf(&this.log, format+"\n", args...)
}

func (this *DispatchProcessorFixture) Write(ctx context.Context, dispatches ...messaging.Dispatch) (int, error) {
	if this.writeBlocks {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	this.writeCount++
	this.writeContext = ctx
	this.writeDispatches = append(this.writeDispatches, dispatches...)

	if this.writeFailureUntil > this.writeCount-1 {
		return 0, this.writeError
	} else {
		return len(dispatches), nil
	}
}
func (this *DispatchProcessorFixture) Close() error {
	this.closeCount++
	return nil
}

func (this *DispatchProcessorFixture) Load(ctx context.Context, id uint64) ([]messaging.Dispatch, error) {
	this.loadCount++
	this.loadContext = ctx
	this.loadID = id

	if id >= uint64(len(this.loadResult)) {
		return nil, nil
	}

	if this.loadMaxResultsPer == 0 || len(this.loadResult) <= this.loadMaxResultsPer {
		return this.loadResult, this.loadError
	}
	result := this.loadResult[id:]
	if len(result) <= this.loadMaxResultsPer {
		return result, this.loadError
	}

	return result[0:this.loadMaxResultsPer], this.loadError
}
func (this *DispatchProcessorFixture) Confirm(ctx context.Context, dispatches []messaging.Dispatch) error {
	this.confirmCount++
	this.confirmContext = ctx
	this.confirmDispatches = append(this.confirmDispatches, dispatches...)
	if this.confirmFailureUntil > this.confirmCount-1 {
		return this.confirmError
	} else {
		return nil
	}
}
func (this *DispatchProcessorFixture) Store(ctx context.Context, writer adapter.Writer, dispatches []messaging.Dispatch) error {
	panic("nop")
}
