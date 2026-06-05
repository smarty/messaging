package harness

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
)

func TestPersistenceFixture(t *testing.T) {
	gunit.Run(new(PersistenceFixture), t)
}

type PersistenceFixture struct {
	*gunit.Fixture
	ctx     context.Context
	input   chan *unitOfWork
	output  chan *unitOfWork
	waits   []time.Duration
	waitErr error
	subject *persistence

	writeMu        sync.Mutex
	writeCalls     [][]*Message
	writeFailCount int

	tracked []any
}

func (this *PersistenceFixture) Setup() {
	this.ctx = context.WithValue(this.Context(), "testing", this.Name())
	this.input = make(chan *unitOfWork, 4)
	this.output = make(chan *unitOfWork, 4)
	this.subject = newPersistence(this.ctx, this, this.input, this.output, this, this.wait)
}

func (this *PersistenceFixture) wait(_ context.Context, d time.Duration) error {
	this.waits = append(this.waits, d)
	return this.waitErr
}

func (this *PersistenceFixture) Track(observation any) {
	this.tracked = append(this.tracked, observation)
}

func (this *PersistenceFixture) Write(ctx context.Context, messages ...*Message) error {
	this.So(ctx.Value("testing"), should.Equal, this.Name())
	this.writeMu.Lock()
	defer this.writeMu.Unlock()
	captured := make([]*Message, len(messages))
	copy(captured, messages)
	this.writeCalls = append(this.writeCalls, captured)
	if this.writeFailCount > 0 {
		this.writeFailCount--
		return errors.New("write failure")
	}
	return nil
}

func (this *PersistenceFixture) drain() (results []*unitOfWork) {
	for unit := range this.output {
		results = append(results, unit)
	}
	return results
}

func (this *PersistenceFixture) TestWritesAllResultsThenForwardsUnit() {
	m1 := &Message{Value: "a"}
	m2 := &Message{Value: "b"}
	this.input <- &unitOfWork{results: []*Message{m1, m2}}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 1)
	this.So(len(this.writeCalls), should.Equal, 1)
	this.So(this.writeCalls[0], should.Equal, []*Message{m1, m2})
	this.So(this.waits, should.BeEmpty)
	this.So(this.tracked, should.BeEmpty)
}

func (this *PersistenceFixture) TestEachUnitIsWrittenIndependently() {
	m1 := &Message{Value: "a"}
	m2 := &Message{Value: "b"}
	this.input <- &unitOfWork{results: []*Message{m1}}
	this.input <- &unitOfWork{results: []*Message{m2}}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 2)
	this.So(len(this.writeCalls), should.Equal, 2)
	this.So(this.writeCalls[0], should.Equal, []*Message{m1})
	this.So(this.writeCalls[1], should.Equal, []*Message{m2})
	this.So(this.tracked, should.BeEmpty)
}

func (this *PersistenceFixture) TestEmptyResultsTriggersEmptyWrite() {
	this.input <- &unitOfWork{}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 1)
	this.So(len(this.writeCalls), should.Equal, 1)
	this.So(this.writeCalls[0], should.BeEmpty)
	this.So(this.tracked, should.BeEmpty)
}

func (this *PersistenceFixture) TestRetriesUntilWriteSucceeds() {
	this.writeFailCount = 2
	m := &Message{Value: "retried"}
	this.input <- &unitOfWork{results: []*Message{m}}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 1)
	this.So(len(this.writeCalls), should.Equal, 3)
	this.So(this.waits, should.Equal, []time.Duration{time.Second, time.Second})
	this.So(this.tracked, should.HaveLength, 2)
	for n, observation := range this.tracked {
		failure, ok := observation.(PersistenceError)
		this.So(ok, should.BeTrue)
		this.So(failure.Error, should.WrapError, ErrPersistence)
		this.So(failure.Attempt, should.Equal, n+1)
	}
}

func (this *PersistenceFixture) TestPersistenceAbandonsOnContextCancelAndDropsUnit() {
	this.writeFailCount = 1 << 30 // always fail
	this.waitErr = context.Canceled
	unit := &unitOfWork{results: []*Message{{Value: "abandoned"}}}
	this.input <- unit
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(units, should.BeEmpty)
	this.So(len(this.writeCalls), should.Equal, 1)
	this.So(this.waits, should.Equal, []time.Duration{time.Second})
	this.So(this.tracked, should.HaveLength, 2)
	failure, ok := this.tracked[0].(PersistenceError)
	this.So(ok, should.BeTrue)
	this.So(failure.Error, should.WrapError, ErrPersistence)
	this.So(failure.Attempt, should.Equal, 1)
	this.So(this.tracked[1], should.Equal, PersistenceAbandoned{Attempts: 1})
}

func (this *PersistenceFixture) TestClosedInputClosesOutput() {
	close(this.input)
	go this.subject.Listen()

	_, open := <-this.output
	this.So(open, should.BeFalse)
	this.So(this.tracked, should.BeEmpty)
}
