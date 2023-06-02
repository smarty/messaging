package serialization

import (
	"errors"
	"reflect"
	"testing"

	"github.com/smarty/messaging/v3"
	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
)

func TestDeliveryDecoderFixture(t *testing.T) {
	gunit.Run(new(DeliveryDecoderFixture), t)
}

type DeliveryDecoderFixture struct {
	*gunit.Fixture

	config configuration

	readTypes     map[string]reflect.Type
	deserializers map[string]Deserializer
	delivery      messaging.Delivery

	decoder DeliveryDecoder

	deserializeCalls    int
	deserializePayload  []byte
	deserializeInstance any
	deserializeError    error
}

func (this *DeliveryDecoderFixture) Setup() {
	this.readTypes = map[string]reflect.Type{}
	this.deserializers = map[string]Deserializer{}
	this.config = configuration{Deserializers: this.deserializers}
	this.newDecoder()
}
func (this *DeliveryDecoderFixture) newDecoder() {
	Options.apply(Options.ReadTypes(this.readTypes),
		Options.IgnoreUnknownMessageTypes(this.config.IgnoreUnknownMessageTypes),
		Options.IgnoreUnknownContentTypes(this.config.IgnoreUnknownContentTypes),
		Options.IgnoreDeserializationErrors(this.config.IgnoreDeserializationErrors),
	)(&this.config)
	this.decoder = newDeliveryDecoder(this.config)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *DeliveryDecoderFixture) TestWhenMessageTypeNotFound_ReturnError() {
	this.delivery.Payload = []byte("payload")

	err := this.decoder.Decode(&this.delivery)

	this.So(err, should.Wrap, ErrSerializationFailure)
}

func (this *DeliveryDecoderFixture) TestWhenContentTypeNotFound_ReturnError() {
	this.delivery.MessageType = "found"
	this.delivery.ContentType = "not-found"
	this.delivery.Payload = []byte("payload")
	this.readTypes["found"] = reflect.TypeOf(0)

	err := this.decoder.Decode(&this.delivery)

	this.So(err, should.Wrap, ErrSerializationFailure)
}

func (this *DeliveryDecoderFixture) TestWhenDeserializationFails_ReturnError() {
	this.delivery.MessageType = "found"
	this.delivery.ContentType = "found"
	this.delivery.Payload = []byte("payload")
	this.readTypes["found"] = reflect.TypeOf(0)
	this.deserializers["found"] = this
	this.deserializeError = errors.New("error")

	err := this.decoder.Decode(&this.delivery)

	this.So(err, should.Wrap, ErrSerializationFailure)
	this.So(this.deserializeCalls, should.Equal, 1)
	this.So(this.deserializePayload, should.Resemble, this.delivery.Payload)
}

func (this *DeliveryDecoderFixture) TestWhenDecodingSucceeds_PopulateMessageOnDelivery() {
	this.delivery.MessageType = "found"
	this.delivery.ContentType = "found"
	this.delivery.Payload = []byte("payload")
	this.readTypes["found"] = reflect.TypeOf(0)
	this.deserializers["found"] = this

	err := this.decoder.Decode(&this.delivery)

	this.So(err, should.BeNil)
	this.So(this.delivery.Message, should.Equal, 42)
	this.So(this.deserializeCalls, should.Equal, 1)
	this.So(this.deserializePayload, should.Resemble, this.delivery.Payload)
}

func (this *DeliveryDecoderFixture) TestWhenDeliveryHasNoBody_SkipDecoding() {
	this.delivery.Payload = nil

	err := this.decoder.Decode(&this.delivery)

	this.So(err, should.BeNil)
	this.So(this.delivery.Message, should.BeNil)
	this.So(this.deserializeCalls, should.BeZeroValue)
}

func (this *DeliveryDecoderFixture) TestWhenDeliveryAlreadyHasMessage_SkipDecoding() {
	this.delivery.Payload = []byte{0x0}
	this.delivery.Message = "exists"

	err := this.decoder.Decode(&this.delivery)

	this.So(err, should.BeNil)
	this.So(this.deserializeCalls, should.BeZeroValue)
}

func (this *DeliveryDecoderFixture) TestWhenDeliveryMessageTypeNotFound_IgnoreUnknownMessageType_SkipDecoding() {
	this.config.IgnoreUnknownMessageTypes = true
	this.newDecoder()
	this.delivery.MessageType = "not found"
	this.delivery.Payload = []byte("payload")

	err := this.decoder.Decode(&this.delivery)

	this.So(err, should.BeNil)
	this.So(this.delivery.Message, should.BeNil)
	this.So(this.deserializeCalls, should.Equal, 0)
}

func (this *DeliveryDecoderFixture) TestWhenDeliveryContentTypeNotFound_IgnoreUnknownContentType_SkipDecoding() {
	this.config.IgnoreUnknownContentTypes = true
	this.newDecoder()
	this.delivery.MessageType = "found"
	this.delivery.ContentType = "not-found"
	this.delivery.Payload = []byte("payload")
	this.readTypes["found"] = reflect.TypeOf(0)

	err := this.decoder.Decode(&this.delivery)

	this.So(err, should.BeNil)
	this.So(this.delivery.Message, should.BeNil)
	this.So(this.deserializeCalls, should.Equal, 0)
}

func (this *DeliveryDecoderFixture) TestWhenDeserializationFails_IgnoreDeserializationError_Success() {
	this.config.IgnoreDeserializationErrors = true
	this.newDecoder()
	this.delivery.MessageType = "found"
	this.delivery.ContentType = "found"
	this.delivery.Payload = []byte("payload")
	this.readTypes["found"] = reflect.TypeOf(0)
	this.deserializers["found"] = this
	this.deserializeError = errors.New("error")

	err := this.decoder.Decode(&this.delivery)

	this.So(err, should.BeNil)
	this.So(this.delivery.Message, should.BeNil)
	this.So(this.deserializeCalls, should.Equal, 1)
}

func (this *DeliveryDecoderFixture) TestWhenMessageTypeNotAllowed_ReturnError() {
	this.config.AllowedTypes = map[string]struct{}{"only-this-type-is-allowed": {}}
	this.newDecoder()
	this.delivery.MessageType = "found" // this type isn't allowed because it's not in the above list
	this.delivery.ContentType = "found"
	this.delivery.Payload = []byte("payload")
	this.readTypes["found"] = reflect.TypeOf(0)
	this.deserializers["found"] = this

	err := this.decoder.Decode(&this.delivery)

	this.So(err, should.Equal, ErrMessageTypeNotAllowed)
	this.So(this.delivery.Message, should.BeNil)
	this.So(this.deserializeCalls, should.Equal, 0)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *DeliveryDecoderFixture) ContentType() string { panic("not called") }

func (this *DeliveryDecoderFixture) Deserialize(raw []byte, instance any) error {
	this.deserializeCalls++
	this.deserializePayload = raw
	this.deserializeInstance = instance
	if this.deserializeError != nil {
		return this.deserializeError
	}

	*(instance.(*int)) = 42
	return nil
}
