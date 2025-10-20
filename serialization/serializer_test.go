package serialization

import (
	"testing"

	"github.com/smarty/gunit"
	"github.com/smarty/gunit/assert/should"
)

func TestSerializerFixture(t *testing.T) {
	gunit.Run(new(SerializerFixture), t)
}

type SerializerFixture struct {
	*gunit.Fixture
	serializer defaultSerializer
}

func (this *SerializerFixture) TestSerializeAndDeserialize() {
	var actual = "Hello, World!"
	var expected = ""

	payload, serializeError := this.serializer.Serialize(actual)
	deserializeError := this.serializer.Deserialize(payload, &expected)

	this.So(this.serializer.ContentType(), should.Equal, "application/json")
	this.So(serializeError, should.BeNil)
	this.So(deserializeError, should.BeNil)
	this.So(expected, should.Equal, actual)
}

func (this *SerializerFixture) TestWhenSerializationFails_ExpectedErrorReturned() {
	source := func() {}

	payload, err := this.serializer.Serialize(source)

	this.So(payload, should.BeNil)
	this.So(err, should.Wrap, ErrSerializeUnsupportedType)
}

func (this *SerializerFixture) TestWhenDeserializationFails_ExpectedErrorReturned() {
	var target int

	err := this.serializer.Deserialize([]byte("Hello"), &target)

	this.So(err, should.Wrap, ErrDeserializeMalformedPayload)
}
