package sqlmq

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/messaging/v3"
	"github.com/smartystreets/messaging/v3/sqlmq/adapter"
)

func TestDispatchStoreFixture(t *testing.T) {
	gunit.Run(new(DispatchStoreFixture), t)
}

type DispatchStoreFixture struct {
	*gunit.Fixture

	now   time.Time
	ctx   context.Context
	store messageStore

	execCalls     int
	execContext   context.Context
	execStatement string
	execArgs      []interface{}
	execError     error

	queryContext   context.Context
	queryStatement string
	queryArgs      []interface{}
	queryError     error
	queryResult    *storageQueryResult

	lastInsertID      int64
	rowsAffectedValue int64
}

func (this *DispatchStoreFixture) Setup() {
	this.now = time.Now().UTC()
	this.ctx = context.Background()
	this.store = newMessageStore(this, 7, func() time.Time { return this.now })
}

func (this *DispatchStoreFixture) TestWhenNoDispatchesToWrite_DoNotPerformWriteOperation() {
	err := this.store.Store(this.ctx, this, nil)

	this.So(err, should.BeNil)
	this.So(this.execCalls, should.BeZeroValue)
}
func (this *DispatchStoreFixture) TestWhenStoring_WriteToUnderlyingStoreAndMarkDispatchesWithMessageID() {
	// https://dev.mysql.com/doc/refman/5.6/en/information-functions.html#function_last-insert-id
	// > If you insert multiple rows using a single INSERT statement, LAST_INSERT_ID() returns the value generated for
	// > the first inserted row only.
	this.rowsAffectedValue = 3
	this.lastInsertID = 42
	writes := []messaging.Dispatch{
		{MessageType: "1", Payload: []byte("a")},
		{MessageType: "2", Payload: []byte("b")},
		{MessageType: "3", Payload: []byte("c")},
	}

	err := this.store.Store(this.ctx, this, writes)

	this.So(err, should.BeNil)

	this.So(this.execContext, should.Equal, this.ctx)
	this.So(this.execStatement, should.Equal, "INSERT INTO Messages (type, payload) VALUES (?,?),(?,?),(?,?);")
	this.So(this.execArgs, should.Resemble, []interface{}{
		"1", []byte("a"),
		"2", []byte("b"),
		"3", []byte("c"),
	})

	this.So(writes, should.Resemble, []messaging.Dispatch{
		{MessageID: 42, MessageType: "1", Payload: []byte("a")},
		{MessageID: 49, MessageType: "2", Payload: []byte("b")},
		{MessageID: 56, MessageType: "3", Payload: []byte("c")},
	})
}
func (this *DispatchStoreFixture) TestWhenStoreWriteFails_ReturnErrorDoNotCommitOrSendToOutputChannel() {
	this.execError = errors.New("")

	err := this.store.Store(this.ctx, this, []messaging.Dispatch{{MessageType: "1", Payload: []byte("a")}})

	this.So(err, should.Equal, this.execError)
}
func (this *DispatchStoreFixture) TestWhenRowsAffectedDoesNotMatchWrites_ReturnErrorDoNotCommitAndDoNotSendToOutputChannel() {
	this.rowsAffectedValue = 0
	this.lastInsertID = 1

	err := this.store.Store(this.ctx, this, []messaging.Dispatch{{MessageType: "1", Payload: []byte("a")}})

	this.So(err, should.Equal, errRowsAffected)
}
func (this *DispatchStoreFixture) TestWhenLastInsertIDCannotBeDetermined_ReturnErrorDoNotCommitAndDoNotSendToOutputChannel() {
	this.rowsAffectedValue = 1
	this.lastInsertID = 0

	err := this.store.Store(this.ctx, this, []messaging.Dispatch{{MessageType: "1", Payload: []byte("a")}})

	this.So(err, should.Equal, errIdentityFailure)
}

func (this *DispatchStoreFixture) TestWhenLoading_ItShouldQueryUnderlingStorage() {
	expected := []messaging.Dispatch{
		{MessageID: 42, MessageType: "message-type1", Payload: []byte{4}},
		{MessageID: 43, MessageType: "message-type2", Payload: []byte{5}},
		{MessageID: 44, MessageType: "message-type3", Payload: []byte{6}},
	}
	this.queryResult = &storageQueryResult{items: expected}

	results, err := this.store.Load(this.ctx, 42)

	this.So(results, should.Resemble, []messaging.Dispatch{
		{MessageID: 42, MessageType: "message-type1", Topic: "message-type1", Payload: []byte{4}, Timestamp: this.now, Durable: true, ContentType: "application/json"},
		{MessageID: 43, MessageType: "message-type2", Topic: "message-type2", Payload: []byte{5}, Timestamp: this.now, Durable: true, ContentType: "application/json"},
		{MessageID: 44, MessageType: "message-type3", Topic: "message-type3", Payload: []byte{6}, Timestamp: this.now, Durable: true, ContentType: "application/json"},
	})
	this.So(err, should.BeNil)
	this.So(this.queryStatement, should.Equal, "SELECT id, type, payload FROM Messages WHERE dispatched IS NULL AND id > 42;")
	this.So(this.queryArgs, should.BeEmpty)
	this.So(this.queryResult.closeCount, should.Equal, 1)
}
func (this *DispatchStoreFixture) TestWhenLoadingQueryFails_ItShouldReturnError() {
	this.queryError = errors.New("")

	results, err := this.store.Load(this.ctx, 42)

	this.So(results, should.BeEmpty)
	this.So(err, should.Equal, this.queryError)
}
func (this *DispatchStoreFixture) TestWhenLoadingQueryScanFails_ItShouldReturnError() {
	this.queryResult = &storageQueryResult{
		items:     []messaging.Dispatch{{}},
		scanError: errors.New(""),
	}

	results, err := this.store.Load(this.ctx, 42)

	this.So(results, should.BeEmpty)
	this.So(err, should.Equal, this.queryResult.scanError)
	this.So(this.queryResult.closeCount, should.Equal, 1)
}
func (this *DispatchStoreFixture) TestWhenLoadingQueryRowIterationsFails_ItShouldReturnError() {
	this.queryResult = &storageQueryResult{errError: errors.New("")}

	results, err := this.store.Load(this.ctx, 42)

	this.So(results, should.BeEmpty)
	this.So(err, should.Equal, this.queryResult.errError)
	this.So(this.queryResult.closeCount, should.Equal, 1)
}

func (this *DispatchStoreFixture) TestConfirmNothing_NoOperationsPerformed() {
	err := this.store.Confirm(this.ctx, nil)

	this.So(err, should.BeEmpty)
	this.So(this.execCalls, should.BeZeroValue)
}
func (this *DispatchStoreFixture) TestConfirmedDispatches_WrittenToUnderlyingStorage() {
	this.now = time.Date(2020, 01, 02, 12, 30, 15, 37, time.UTC)
	this.execError = errors.New("")
	writes := []messaging.Dispatch{{MessageID: 1}, {MessageID: 2}, {MessageID: 3}}

	err := this.store.Confirm(this.ctx, writes)

	this.So(err, should.Equal, this.execError)
	this.So(this.execContext, should.Equal, this.ctx)
	this.So(this.execArgs, should.BeEmpty)
	this.So(this.execStatement, should.Equal,
		"UPDATE Messages SET dispatched = '2020-01-02 12:30:15.000000' WHERE dispatched IS NULL AND id IN (1, 2, 3);")
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *DispatchStoreFixture) ExecContext(ctx context.Context, statement string, args ...interface{}) (sql.Result, error) {
	this.execCalls++
	this.execContext = ctx
	this.execStatement = statement
	this.execArgs = args
	return this, this.execError
}
func (this *DispatchStoreFixture) LastInsertId() (int64, error) {
	return this.lastInsertID, nil
}
func (this *DispatchStoreFixture) RowsAffected() (int64, error) {
	return this.rowsAffectedValue, nil
}

func (this *DispatchStoreFixture) QueryContext(ctx context.Context, statement string, args ...interface{}) (adapter.QueryResult, error) {
	this.queryContext = ctx
	this.queryStatement = statement
	this.queryArgs = args
	return this.queryResult, this.queryError
}

func (this *DispatchStoreFixture) QueryRowContext(ctx context.Context, statement string, args ...interface{}) adapter.RowScanner {
	panic("nop")
}

type storageQueryResult struct {
	scanError  error
	errError   error
	index      int
	closeCount int
	items      []messaging.Dispatch
}

func (this *storageQueryResult) Scan(fields ...interface{}) error {
	item := this.items[this.index-1]
	for i := 0; i < len(fields); i++ {
		switch i {
		case 0:
			*(fields[i].(*uint64)) = item.MessageID
		case 1:
			*(fields[i].(*string)) = item.MessageType
		case 2:
			*(fields[i].(*[]byte)) = item.Payload
		default:
			panic("bad scan")
		}
	}
	return this.scanError
}
func (this *storageQueryResult) Next() bool {
	this.index++
	return this.index <= len(this.items)
}
func (this *storageQueryResult) Err() error {
	return this.errError
}
func (this *storageQueryResult) Close() error {
	this.closeCount++
	return nil
}
