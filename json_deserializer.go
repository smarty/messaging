package messaging

import (
	"encoding/json"
	"log"
	"reflect"
)

type Transformer interface {
	Transform(*Delivery)
}

type JSONDeserializer struct {
	types map[string]reflect.Type

	panicMissingType bool
	panicUnmarshal   bool
}

func NewJSONDeserializer(types map[string]reflect.Type) *JSONDeserializer {
	return &JSONDeserializer{types: types}
}

func (this *JSONDeserializer) PanicWhenMessageTypeIsUnknown() {
	this.panicMissingType = true
}

func (this *JSONDeserializer) PanicWhenDeserializationFails() {
	this.panicUnmarshal = true
}

func (this *JSONDeserializer) Transform(delivery *Delivery) {
	this.Deserialize(delivery)
}

func (this *JSONDeserializer) Deserialize(delivery *Delivery) {
	messageType, found := this.types[delivery.MessageType]
	if !found && this.panicMissingType {
		log.Panicf("MessageType not found: '%s'", delivery.MessageType)
	} else if !found {
		log.Printf("[WARN] MessageType not found: '%s'", delivery.MessageType)
		return
	}

	pointer := reflect.New(messageType)
	err := json.Unmarshal(delivery.Payload, pointer.Interface())
	if err != nil && this.panicUnmarshal {
		log.Panicf("Could not deserialize message of type '%s': %s", delivery.MessageType, err.Error())
	} else if err != nil {
		log.Printf("[WARN] Could not deserialize message of type '%s': %s", delivery.MessageType, err.Error())
		return
	}

	delivery.Message = pointer.Elem().Interface()
}
