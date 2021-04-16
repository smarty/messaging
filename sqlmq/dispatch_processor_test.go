package sqlmq

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/messaging/v4"
	"github.com/smartystreets/messaging/v4/sqlmq/adapter"
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

	writeCount        int
	writeFailureUntil int
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
	)
}
func (this *DispatchProcessorFixture) listen(sleep time.Duration) {
	go func() {
		time.Sleep(sleep)
		_ = this.listener.Close()
	}()

	this.listener.Listen() // blocks
}

func (this *DispatchProcessorFixture) TestWhenClose_ShutDownChannelAndAllowListenToExit() {
	this.listen(time.Millisecond)

	_, open := <-this.channel

	this.So(this.closeCount, should.Equal, 1)
	this.So(open, should.BeFalse)
}
func (this *DispatchProcessorFixture) SkipTestWhenDispatchesArePending_ItShouldPublishThemAndConfirmDispatch() {
	expected := []messaging.Dispatch{{MessageID: 1}, {MessageID: 2}, {MessageID: 3}}
	for _, item := range expected {
		this.channel <- item
	}

	this.listen(time.Millisecond)

	this.So(this.writeCount, should.Equal, 1)
	this.So(this.writeContext, should.NotBeNil)
	this.So(this.writeDispatches, should.Resemble, expected)

	this.So(this.confirmCount, should.Equal, 1)
	this.So(this.confirmContext, should.NotBeNil)
	this.So(this.confirmDispatches, should.Resemble, expected)
}
func (this *DispatchProcessorFixture) SkipTestWhenWritingToSenderFails_TryAgain() {
	this.writeError = errors.New("")
	this.writeFailureUntil = 1 // 0th attempt fails, subsequent attempts pass

	expected := []messaging.Dispatch{{MessageID: 1}, {MessageID: 2}, {MessageID: 3}}
	for _, item := range expected {
		this.channel <- item
	}

	this.listen(time.Millisecond * 3)

	this.So(this.writeCount, should.Equal, 2)
	this.So(this.writeContext, should.NotBeNil)
	this.So(this.writeDispatches, should.Resemble, append(expected, expected...))

	this.So(this.confirmCount, should.Equal, 1)
	this.So(this.confirmContext, should.NotBeNil)
	this.So(this.confirmDispatches, should.Resemble, expected)
}
func (this *DispatchProcessorFixture) SkipTestWhenConfirmDispatchFails_TryAgainWithoutWritingToSenderTwice() {
	this.confirmError = errors.New("")
	this.confirmFailureUntil = 1 // 0th attempt fails, subsequent attempts pass

	expected := []messaging.Dispatch{{MessageID: 1}, {MessageID: 2}, {MessageID: 3}}
	for _, item := range expected {
		this.channel <- item
	}

	this.listen(time.Millisecond * 10)

	this.So(this.writeCount, should.Equal, 1)
	this.So(this.writeContext, should.NotBeNil)
	this.So(this.writeDispatches, should.Resemble, expected)

	this.So(this.confirmCount, should.Equal, 2)
	this.So(this.confirmContext, should.NotBeNil)
	this.So(this.confirmDispatches, should.Resemble, append(expected, expected...))
}
func (this *DispatchProcessorFixture) SkipTestWhenClosingDuringSleepRetry_SleepIsCutShort() {
	this.sleepTimeout = time.Minute
	this.initializeDispatchProcessor()

	this.confirmError = errors.New("") // sleep begins on error for 1 min
	this.channel <- messaging.Dispatch{}

	start := time.Now().UTC()
	this.listen(time.Millisecond * 3)
	completed := time.Since(start)

	this.So(completed, should.BeLessThan, time.Millisecond*10)
}

func (this *DispatchProcessorFixture) SkipTestWhenListening_ReadPendingDispatchesFromStorage() {
	expected := []messaging.Dispatch{{MessageID: 1}, {MessageID: 2}, {MessageID: 3}}
	this.loadResult = expected

	this.listen(time.Millisecond * 50)

	this.So(this.writeCount, should.Equal, 1)
	this.So(this.writeContext, should.NotBeNil)
	this.So(this.writeDispatches, should.Resemble, expected)

	this.So(this.confirmCount, should.Equal, 1)
	this.So(this.confirmContext, should.NotBeNil)
	this.So(this.confirmDispatches, should.Resemble, expected)
}
func (this *DispatchProcessorFixture) SkipTestWhenInitializeOperationDoesNotGetAllPendingDispatches_ReadFromLastSuccess() {
	this.loadResult = []messaging.Dispatch{{MessageID: 1}, {MessageID: 2}, {MessageID: 3}, {MessageID: 4}, {MessageID: 5}}
	this.loadMaxResultsPer = 2
	this.loadError = errors.New("")

	this.listen(time.Millisecond * 25)

	this.So(this.writeCount, should.Equal, 3)
	this.So(this.writeContext, should.NotBeNil)
	this.So(this.writeDispatches, should.Resemble, this.loadResult)

	this.So(this.confirmCount, should.Equal, 3)
	this.So(this.confirmContext, should.NotBeNil)
	this.So(this.confirmDispatches, should.Resemble, this.loadResult)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *DispatchProcessorFixture) Write(ctx context.Context, dispatches ...messaging.Dispatch) (int, error) {
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
