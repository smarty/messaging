package sqladapter

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/messaging/v3/handlers/harness"
)

// Local test-only message types mirror the shape the source used.
type (
	orderReceived struct {
		AccountID uint64
		OrderID   uint64
		Timestamp time.Time
	}
	orderApproved struct {
		AccountID uint64
		OrderID   uint64
		Timestamp time.Time
	}
)

func TestWriterFixture(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running database tests.")
	}
	ensureDatabaseReadiness(t)
	gunit.Run(new(WriterFixture), t, gunit.Options.IntegrationTests())
}

type WriterFixture struct {
	*gunit.Fixture
	handle  *sql.DB
	subject *Writer

	legacyWriteCalls [][]any
	legacyWritePanic any
	testStride       uint64
}

func (this *WriterFixture) Setup() {
	handle, err := openTestDatabase()
	this.So(err, should.BeNil)
	this.handle = handle
	this.legacyWriteCalls = nil
	this.legacyWritePanic = nil
	this.testStride = 7
	this.subject = NewWriter(handle, testTypeNames(), this.testStride, this, this.fakeLegacyWrite)
	this.truncateTables()
}

func (this *WriterFixture) fakeLegacyWrite(_ context.Context, _ *sql.Tx, messages ...any) {
	this.legacyWriteCalls = append(this.legacyWriteCalls, messages)
	if this.legacyWritePanic != nil {
		panic(this.legacyWritePanic)
	}
}

func (this *WriterFixture) Teardown() {
	_ = this.handle.Close()
}

func (this *WriterFixture) truncateTables() {
	_, err := this.handle.Exec(`TRUNCATE TABLE Messages;`)
	this.So(err, should.BeNil)
}

func (this *WriterFixture) TestWrite_InsertsMessageRowAndInvokesLegacyWrite() {
	event := orderReceived{AccountID: 1, OrderID: 2, Timestamp: time.Now().UTC().Round(time.Second)}
	message := serializedMessage(event, "")

	err := this.subject.Write(context.Background(), message)

	this.So(err, should.BeNil)
	this.So(this.countMessages(), should.Equal, 1)
	this.So(this.legacyWriteCalls, should.Equal, [][]any{{message}})
	this.So(message.Type, should.Equal, "order-received")
}

func (this *WriterFixture) TestWrite_ResolvesMessageTypeFromRegistry() {
	event := orderApproved{AccountID: 1, OrderID: 2, Timestamp: time.Now().UTC()}
	message := serializedMessage(event, "")

	err := this.subject.Write(context.Background(), message)

	this.So(err, should.BeNil)
	this.So(this.firstMessageType(), should.Equal, "order-approved")
}

func (this *WriterFixture) TestWrite_LegacyWritePanic_RollsBackInsertedMessage() {
	this.legacyWritePanic = "boom"
	event := orderReceived{AccountID: 1, OrderID: 2, Timestamp: time.Now().UTC()}

	err := this.subject.Write(context.Background(), serializedMessage(event, ""))

	this.So(err, should.NOT.BeNil)
	this.So(this.countMessages(), should.Equal, 0)
}

func (this *WriterFixture) TestWrite_PreservesPreSetMessageType() {
	event := orderReceived{AccountID: 1, OrderID: 2, Timestamp: time.Now().UTC()}
	message := serializedMessage(event, "explicit.override")

	err := this.subject.Write(context.Background(), message)

	this.So(err, should.BeNil)
	this.So(this.firstMessageType(), should.Equal, "explicit.override")
}

func (this *WriterFixture) TestWrite_PopulatesMessageIDFromAutoincrement() {
	event1 := orderReceived{AccountID: 1, OrderID: 2, Timestamp: time.Now().UTC()}
	event2 := orderReceived{AccountID: 1, OrderID: 3, Timestamp: time.Now().UTC()}
	m1 := serializedMessage(event1, "")
	m2 := serializedMessage(event2, "")

	err := this.subject.Write(context.Background(), m1, m2)

	this.So(err, should.BeNil)
	this.So(m1.ID, should.NOT.Equal, uint64(0))
	this.So(m2.ID, should.Equal, m1.ID+this.testStride)
}

func (this *WriterFixture) TestWrite_NoMessages_NoOp() {
	err := this.subject.Write(context.Background())
	this.So(err, should.BeNil)
	this.So(this.countMessages(), should.Equal, 0)
}

func (this *WriterFixture) countMessages() int {
	var count int
	err := this.handle.QueryRow(`SELECT COUNT(*) FROM Messages`).Scan(&count)
	this.So(err, should.BeNil)
	return count
}
func (this *WriterFixture) firstMessageType() string {
	var t string
	err := this.handle.QueryRow(`SELECT type FROM Messages ORDER BY id LIMIT 1`).Scan(&t)
	this.So(err, should.BeNil)
	return t
}

func testTypeNames() map[reflect.Type]string {
	return map[reflect.Type]string{
		reflect.TypeOf(orderReceived{}): "order-received",
		reflect.TypeOf(orderApproved{}): "order-approved",
	}
}

func serializedMessage(value any, typeOverride string) *harness.Message {
	payload, _ := json.Marshal(value)
	return &harness.Message{
		Value:       value,
		Type:        typeOverride,
		Content:     bytes.NewBuffer(payload),
		ContentType: "application/json",
	}
}
