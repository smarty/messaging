package streaming

import (
	"context"
	"errors"
	"testing"

	"github.com/smarty/messaging/v4"
	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
)

func TestConnectionPoolFixture(t *testing.T) {
	gunit.Run(new(ConnectionPoolFixture), t)
}

type ConnectionPoolFixture struct {
	*gunit.Fixture

	ctx  context.Context
	pool connectionPool

	connectCount   int
	connectContext context.Context
	connectError   error
	opened         []*fakeConnection
}

func (this *ConnectionPoolFixture) Setup() {
	this.ctx = context.Background()
	this.pool = newConnectionPool(this)
}

func (this *ConnectionPoolFixture) TestWhenConnectFails_ItShouldReturnError() {
	this.connectError = errors.New("")

	connection, err := this.pool.Active(this.ctx)

	this.So(connection, should.BeNil)
	this.So(err, should.Equal, this.connectError)
	this.So(this.connectContext, should.Equal, this.ctx)
}
func (this *ConnectionPoolFixture) TestWhenConnecting_ReturnUnderlyingConnection() {
	connection, err := this.pool.Active(this.ctx)

	this.So(connection, should.Equal, this.opened[0])
	this.So(err, should.BeNil)
	this.So(this.connectContext, should.Equal, this.ctx)
}
func (this *ConnectionPoolFixture) TestWhenOpeningAConnectionTwice_ItShouldConnectOnceAndGiveSameConnectionInstance() {
	first, _ := this.pool.Active(this.ctx)

	second, err := this.pool.Active(this.ctx)

	this.So(first, should.Equal, second)
	this.So(err, should.BeNil)
	this.So(this.connectCount, should.Equal, 1)
}

func (this *ConnectionPoolFixture) TestWhenReleasingAConnection_ItShouldCloseTheReleasedConnection() {
	connection, _ := this.pool.Active(this.ctx)

	this.pool.Dispose(connection)

	this.So(this.opened[0].closeCount, should.Equal, 1)
}
func (this *ConnectionPoolFixture) TestWhenReleasingANilConnection_Nop() {
	this.So(func() { this.pool.Dispose(nil) }, should.NotPanic)
}
func (this *ConnectionPoolFixture) TestWhenReleasingCurrentConnection_ItShouldCauseCurrentToReturnNewConnection() {
	first, _ := this.pool.Active(this.ctx)
	this.pool.Dispose(first)

	second, _ := this.pool.Active(this.ctx)

	this.So(second, should.NotBeNil)
	this.So(first, should.NotEqual, second)
	this.So(this.connectCount, should.Equal, 2)
}
func (this *ConnectionPoolFixture) TestWhenReleasingPriorConnection_ItShouldStillGiveBackCurrentConnection() {
	first, _ := this.pool.Active(this.ctx)
	this.pool.Dispose(first)
	secondA, _ := this.pool.Active(this.ctx)
	this.pool.Dispose(first) // release first again

	secondB, _ := this.pool.Active(this.ctx)

	this.So(secondA, should.Equal, secondB)
	this.So(secondB, should.NotBeNil)
	this.So(this.connectCount, should.Equal, 2)
}

func (this *ConnectionPoolFixture) TestWhenClosing_ReleaseCurrentConnectionIfAny() {
	_, _ = this.pool.Active(this.ctx)

	_ = this.pool.Close()

	this.So(this.opened[0].closeCount, should.Equal, 1)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *ConnectionPoolFixture) Connect(ctx context.Context) (messaging.Connection, error) {
	this.connectCount++
	this.connectContext = ctx
	if this.connectError != nil {
		return nil, this.connectError
	}

	connection := &fakeConnection{}
	this.opened = append(this.opened, connection)
	return connection, nil
}
func (this *ConnectionPoolFixture) Close() error {
	panic("nop")
}

type fakeConnection struct {
	closeCount int
}

func (this *fakeConnection) Close() error {
	this.closeCount++
	return nil
}
func (this *fakeConnection) Reader(ctx context.Context) (messaging.Reader, error) {
	panic("nop")
}
func (this *fakeConnection) Writer(ctx context.Context) (messaging.Writer, error) {
	panic("nop")
}
func (this *fakeConnection) CommitWriter(ctx context.Context) (messaging.CommitWriter, error) {
	panic("nop")
}
