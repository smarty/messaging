package messaging

import (
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"testing"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/logging"
)

func TestJSONDeserializerFixture(t *testing.T) {
	gunit.Run(new(JSONDeserializerFixture), t)
}

type JSONDeserializerFixture struct {
	*gunit.Fixture

	deserializer *JSONDeserializer
}

func (this *JSONDeserializerFixture) Setup() {
	log.SetOutput(ioutil.Discard)

	this.deserializer = NewJSONDeserializer(map[string]reflect.Type{
		"ApplicationEvent":  reflect.TypeOf(ApplicationEvent{}),
		"*ApplicationEvent": reflect.TypeOf(&ApplicationEvent{}),
	})
	this.deserializer.logger = logging.Capture()
}

func (this *JSONDeserializerFixture) Teardown() {
	log.SetOutput(os.Stdout)
}

func (this *JSONDeserializerFixture) TestDeserializeKnownStructMessageType() {
	delivery := &Delivery{
		MessageType: "ApplicationEvent",
		Payload:     []byte(`{"Message": "Hello, World!"}`),
	}

	this.deserializer.Deserialize(delivery)

	this.So(delivery.Message, should.Resemble, ApplicationEvent{Message: "Hello, World!"})
}

func (this *JSONDeserializerFixture) TestDeserializeKnownPointerMessageType() {
	delivery := &Delivery{
		MessageType: "*ApplicationEvent",
		Payload:     []byte(`{"Message": "Hello, World!"}`),
	}

	this.deserializer.Deserialize(delivery)

	this.So(delivery.Message, should.Resemble, &ApplicationEvent{Message: "Hello, World!"})
}

func (this *JSONDeserializerFixture) TestDeserializeUnknownMessageType() {
	delivery := &Delivery{
		MessageType: "What Am I?",
		Payload:     []byte(`{"Message": "Hello, World!"}`),
	}

	this.deserializer.Deserialize(delivery)

	this.So(delivery.Message, should.BeNil)
}

func (this *JSONDeserializerFixture) TestDeserializeUnknownMessageTypeIsCriticalFailure() {
	this.deserializer.PanicWhenMessageTypeIsUnknown()

	delivery := &Delivery{
		MessageType: "What Am I?",
		Payload:     []byte(`{"Message": "Hello, World!"}`),
	}

	this.So(func() { this.deserializer.Deserialize(delivery) }, should.Panic)
}

func (this *JSONDeserializerFixture) TestDeserializationFailsSilently() {
	delivery := &Delivery{
		MessageType: "ApplicationEvent",
		Payload:     []byte(`ThisIsNotJSON`),
	}

	this.deserializer.Deserialize(delivery)

	this.So(delivery.Message, should.BeNil)
}

func (this *JSONDeserializerFixture) TestDeserializationFailsWithPanic() {
	this.deserializer.PanicWhenDeserializationFails()

	delivery := &Delivery{
		MessageType: "ApplicationEvent",
		Payload:     []byte(`blah blah blah`),
	}

	this.So(func() { this.deserializer.Deserialize(delivery) }, should.Panic)
}

///////////////////////////////////////////////////////////////

type ApplicationEvent struct {
	Message string
}
