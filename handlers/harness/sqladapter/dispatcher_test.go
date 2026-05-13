package sqladapter

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"os"
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/messaging/v3"
	"github.com/smarty/messaging/v3/handlers/harness"
)

type dispatcherTestEvent struct {
	AccountID uint64
	OrderID   uint64
}

func TestDispatcherFixture(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running database tests.")
	}
	ensureDatabaseReadiness(t)
	gunit.Run(new(DispatcherFixture), t, gunit.Options.IntegrationTests())
}

type DispatcherFixture struct {
	*gunit.Fixture
	handle    *sql.DB
	connector *stubConnector
	subject   *Dispatcher
}

func (this *DispatcherFixture) Setup() {
	handle, err := openTestDatabase()
	this.So(err, should.BeNil)
	this.handle = handle
	_, err = handle.Exec(`TRUNCATE TABLE Messages;`)
	this.So(err, should.BeNil)
	this.connector = newStubConnector()
	this.subject = NewDispatcher(this.connector, handle, log.New(os.Stderr, "", 0))
}

func (this *DispatcherFixture) Teardown() {
	_ = this.handle.Close()
}

func (this *DispatcherFixture) seedMessage(value any) *harness.Message {
	result, err := this.handle.Exec(`INSERT INTO Messages (type, payload) VALUES ('order-received', '{}')`)
	this.So(err, should.BeNil)
	id, err := result.LastInsertId()
	this.So(err, should.BeNil)
	return &harness.Message{ID: uint64(id), Value: value}
}

func (this *DispatcherFixture) TestDispatch_PublishesAndMarksDispatched() {
	event := dispatcherTestEvent{AccountID: 1, OrderID: 2}
	message := this.seedMessage(event)

	err := this.subject.Dispatch(context.Background(), message)

	this.So(err, should.BeNil)
	this.So(len(this.connector.published), should.Equal, 1)
	this.So(this.connector.published[0].Message, should.Equal, event)
	this.So(this.connector.published[0].Durable, should.BeTrue)
	this.So(this.dispatchedTimestamp(message.ID), should.NOT.BeNil)
}

func (this *DispatcherFixture) TestDispatch_PublishFails_ReturnsErrorWithoutMarkingDispatched() {
	event := dispatcherTestEvent{AccountID: 1, OrderID: 2}
	message := this.seedMessage(event)
	this.connector.writeErr = errors.New("rmq down")

	err := this.subject.Dispatch(context.Background(), message)

	this.So(err, should.NOT.BeNil)
	this.So(this.dispatchedTimestamp(message.ID), should.BeNil)
}

func (this *DispatcherFixture) TestDispatch_NoMessages_NoOp() {
	err := this.subject.Dispatch(context.Background())
	this.So(err, should.BeNil)
	this.So(len(this.connector.published), should.Equal, 0)
}

func (this *DispatcherFixture) dispatchedTimestamp(id uint64) *string {
	var dispatched sql.NullString
	err := this.handle.QueryRow(`SELECT dispatched FROM Messages WHERE id = ?`, id).Scan(&dispatched)
	this.So(err, should.BeNil)
	if dispatched.Valid {
		s := dispatched.String
		return &s
	}
	return nil
}

// stubConnector mimics a transport connector.

type stubConnector struct {
	published    []messaging.Dispatch
	writeBatches []int
	writeErr     error
}

func newStubConnector() *stubConnector {
	return &stubConnector{}
}
func (this *stubConnector) Connect(ctx context.Context) (messaging.Connection, error) {
	return &stubConnection{parent: this}, nil
}
func (this *stubConnector) Close() error { return nil }

type stubConnection struct{ parent *stubConnector }

func (this *stubConnection) Reader(ctx context.Context) (messaging.Reader, error) {
	return nil, errors.New("not implemented")
}
func (this *stubConnection) Writer(ctx context.Context) (messaging.Writer, error) {
	return &stubWriter{parent: this.parent}, nil
}
func (this *stubConnection) CommitWriter(ctx context.Context) (messaging.CommitWriter, error) {
	return nil, errors.New("not implemented")
}
func (this *stubConnection) Close() error { return nil }

type stubWriter struct{ parent *stubConnector }

func (this *stubWriter) Write(ctx context.Context, dispatches ...messaging.Dispatch) (int, error) {
	if this.parent.writeErr != nil {
		return 0, this.parent.writeErr
	}
	this.parent.writeBatches = append(this.parent.writeBatches, len(dispatches))
	this.parent.published = append(this.parent.published, dispatches...)
	return len(dispatches), nil
}
func (this *stubWriter) Close() error { return nil }
