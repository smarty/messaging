package serialization

import (
	"errors"
	"reflect"
	"testing"

	"github.com/smarty/messaging/v3"
	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
)

func TestDispatchEncoderFixture(t *testing.T) {
	gunit.Run(new(DispatchEncoderFixture), t)
}

type DispatchEncoderFixture struct {
	*gunit.Fixture

	encoder DispatchEncoder

	writeTypes        map[reflect.Type]string
	dispatch          messaging.Dispatch
	serializeCalls    int
	serializeInstance any
	serializePayload  []byte
	serializeError    error
}

func (this *DispatchEncoderFixture) Setup() {
	this.writeTypes = map[reflect.Type]string{}
	this.encoder = this.newEncoder()
}
func (this *DispatchEncoderFixture) newEncoder(options ...option) DispatchEncoder {
	config := configuration{Deserializers: map[string]Deserializer{}}
	options = append(options, Options.WriteTypes(this.writeTypes), Options.Serializer(this))
	Options.apply(options...)(&config)
	return newDispatchEncoder(config)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *DispatchEncoderFixture) TestWhenDispatchDoesNotHaveMessageToSerialize_Nop() {
	err := this.encoder.Encode(&this.dispatch)

	this.So(err, should.BeNil)
	this.So(this.dispatch.Payload, should.BeNil)
	this.So(this.serializeCalls, should.BeZeroValue)
}
func (this *DispatchEncoderFixture) TestWhenDispatchAlreadyContainsSerializedPayload_Nop() {
	var payload = []byte("hello, world!")
	this.dispatch.Payload = payload

	err := this.encoder.Encode(&this.dispatch)

	this.So(err, should.BeNil)
	this.So(this.dispatch.Payload, should.Resemble, payload)
	this.So(this.serializeCalls, should.BeZeroValue)
}

func (this *DispatchEncoderFixture) TestWhenEncodingUnknownMessageType_ReturnError() {
	this.dispatch.Message = "string type unknown"

	err := this.encoder.Encode(&this.dispatch)

	this.So(err, should.Wrap, ErrSerializationFailure)
	this.So(this.dispatch.Payload, should.BeNil)
	this.So(this.serializeCalls, should.BeZeroValue)
}
func (this *DispatchEncoderFixture) TestWhenSerializationFails_ReturnError() {
	this.writeTypes[reflect.TypeOf("")] = ""
	this.dispatch.Message = "known type"
	this.serializeError = errors.New("serialize fails")

	err := this.encoder.Encode(&this.dispatch)

	this.So(err, should.Wrap, ErrSerializationFailure)
	this.So(this.dispatch.Payload, should.BeNil)
	this.So(this.serializeCalls, should.Equal, 1)
	this.So(this.serializeInstance, should.Equal, this.dispatch.Message)
}

func (this *DispatchEncoderFixture) TestWhenSerializationSucceeds_DispatchShouldBeFullyPopulated() {
	this.writeTypes[reflect.TypeOf("")] = "message-type"
	this.dispatch.Message = "known type"

	err := this.encoder.Encode(&this.dispatch)

	this.So(err, should.BeNil)
	this.So(this.dispatch.Payload, should.Resemble, this.serializePayload)
	this.So(this.dispatch.ContentType, should.Equal, this.ContentType())
	this.So(this.dispatch.MessageType, should.Equal, "message-type")
	this.So(this.dispatch.Topic, should.Equal, "message-type")
}
func (this *DispatchEncoderFixture) TestWhenDispatchTopicAlreadyPopulated_ItShouldIgnoreTopicAndPopulateOtherFields() {
	this.writeTypes[reflect.TypeOf("")] = "message-type"
	this.dispatch.Message = "known type"
	this.dispatch.Topic = "can't touch this"

	err := this.encoder.Encode(&this.dispatch)

	this.So(err, should.BeNil)
	this.So(this.dispatch.Payload, should.Resemble, this.serializePayload)
	this.So(this.dispatch.ContentType, should.Equal, this.ContentType())
	this.So(this.dispatch.MessageType, should.Equal, "message-type")
	this.So(this.dispatch.Topic, should.Equal, "can't touch this")
}

func (this *DispatchEncoderFixture) TestWhenConfiguredToNotCopyMessageTypeFromTopic_ItShouldIgnoreTheTopic() {
	this.encoder = this.newEncoder(Options.TopicFromMessageType(false))
	this.writeTypes[reflect.TypeOf("")] = "message-type"
	this.dispatch.Message = "known type"

	err := this.encoder.Encode(&this.dispatch)

	this.So(err, should.BeNil)
	this.So(this.dispatch.Topic, should.BeBlank)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *DispatchEncoderFixture) ContentType() string { return "test-content-type" }
func (this *DispatchEncoderFixture) Serialize(instance any) ([]byte, error) {
	this.serializeCalls++
	this.serializeInstance = instance
	return this.serializePayload, this.serializeError
}
