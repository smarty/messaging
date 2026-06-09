package harness

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
)

func TestSerializationFixture(t *testing.T) {
	gunit.Run(new(SerializationFixture), t)
}

type SerializationFixture struct {
	*gunit.Fixture
	input   chan *unitOfWork
	output  chan *unitOfWork
	subject *serialization

	tracked []any

	serializeCalls []any
	serializeFail  map[any]error
}

func (this *SerializationFixture) Setup() {
	this.input = make(chan *unitOfWork, 4)
	this.output = make(chan *unitOfWork, 4)
	this.serializeFail = make(map[any]error)
	this.subject = newSerialization(this, this, this.input, this.output)
}

func (this *SerializationFixture) Track(observation any) {
	this.tracked = append(this.tracked, observation)
}

func (this *SerializationFixture) Serialize(out io.Writer, in any) error {
	this.serializeCalls = append(this.serializeCalls, in)
	if err, ok := this.serializeFail[in]; ok {
		return err
	}
	_, _ = out.Write([]byte("encoded:"))
	switch v := in.(type) {
	case string:
		_, _ = out.Write([]byte(v))
	default:
		_, _ = out.Write([]byte("value"))
	}
	return nil
}

func (this *SerializationFixture) ContentType() string {
	return "test/content-type"
}

func (this *SerializationFixture) drain() (results []*unitOfWork) {
	for unit := range this.output {
		results = append(results, unit)
	}
	return results
}

func (this *SerializationFixture) TestSerializesEachResultValueIntoContent() {
	unit := &unitOfWork{results: []*Message{
		{Value: "alpha", Content: bytes.NewBuffer(nil)},
		{Value: "beta", Content: bytes.NewBuffer(nil)},
	}}
	this.input <- unit
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 1)
	this.So(units[0].results[0].Content.String(), should.Equal, "encoded:alpha")
	this.So(units[0].results[1].Content.String(), should.Equal, "encoded:beta")
	this.So(this.serializeCalls, should.Equal, []any{"alpha", "beta"})
	this.So(this.tracked, should.BeEmpty)
}

func (this *SerializationFixture) TestForwardsUnitsInOrder() {
	this.input <- &unitOfWork{results: []*Message{{Value: "one", Content: bytes.NewBuffer(nil)}}}
	this.input <- &unitOfWork{results: []*Message{{Value: "two", Content: bytes.NewBuffer(nil)}}}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 2)
	this.So(units[0].results[0].Content.String(), should.Equal, "encoded:one")
	this.So(units[1].results[0].Content.String(), should.Equal, "encoded:two")
}

func (this *SerializationFixture) TestEmptyResultsForwardsCleanly() {
	this.input <- &unitOfWork{}
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 1)
	this.So(units[0].results, should.BeEmpty)
	this.So(this.tracked, should.BeEmpty)
}

func (this *SerializationFixture) TestClosedInputClosesOutput() {
	close(this.input)
	go this.subject.Listen()

	_, open := <-this.output
	this.So(open, should.BeFalse)
	this.So(this.tracked, should.BeEmpty)
}

func (this *SerializationFixture) TestSerializesEachResultValueIntoContent_PopulatesContentTypeOnSuccess() {
	unit := &unitOfWork{results: []*Message{
		{Value: "hello", Content: bytes.NewBuffer(nil)},
	}}
	this.input <- unit
	close(this.input)

	go this.subject.Listen()

	units := this.drain()
	this.So(len(units), should.Equal, 1)
	this.So(units[0].results[0].ContentType, should.Equal, "test/content-type")
}

func (this *SerializationFixture) listenAndRecover() (recovered any) {
	defer func() { recovered = recover() }()
	this.subject.Listen()
	return nil
}

func (this *SerializationFixture) TestSerializerErrorTracksThenPanics() {
	type TestMessage struct {
		Value string `json:"value"`
	}
	boom := errors.New("boom")
	bad := TestMessage{
		Value: "unserializable",
	}
	this.serializeFail[bad] = boom
	unit := &unitOfWork{results: []*Message{
		{Value: "good-1", Content: bytes.NewBuffer(nil)},
		{Value: bad, Content: bytes.NewBuffer(nil)},
		{Value: "good-2", Content: bytes.NewBuffer(nil)},
	}}
	this.input <- unit
	close(this.input)

	recovered := this.listenAndRecover()

	this.So(recovered, should.NOT.BeNil)
	err, isError := recovered.(error)
	this.So(isError, should.BeTrue)
	this.So(err, should.WrapError, ErrSerialization)
	this.So(err, should.WrapError, boom)
	this.So(this.tracked, should.HaveLength, 1)
	observation, isObservation := this.tracked[0].(SerializationError)
	this.So(isObservation, should.BeTrue)
	this.So(observation.Error, should.WrapError, ErrSerialization)
	this.So(observation.Value, should.Equal, bad)
	this.So(this.serializeCalls, should.Equal, []any{"good-1", bad})
	this.So(this.drain(), should.BeEmpty) // drain returning proves output was closed during unwind
}

func (this *SerializationFixture) TestUnitsBeforeFailureRemainForwarded() {
	boom := errors.New("boom")
	this.serializeFail["bad"] = boom
	this.input <- &unitOfWork{results: []*Message{{Value: "one", Content: bytes.NewBuffer(nil)}}}
	this.input <- &unitOfWork{results: []*Message{{Value: "bad", Content: bytes.NewBuffer(nil)}}}
	close(this.input)

	recovered := this.listenAndRecover()

	this.So(recovered, should.NOT.BeNil)
	units := this.drain()
	this.So(units, should.HaveLength, 1)
	this.So(units[0].results[0].Content.String(), should.Equal, "encoded:one")
}
