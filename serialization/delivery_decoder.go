package serialization

import (
	"fmt"
	"reflect"

	"github.com/smarty/messaging/v4"
)

type defaultDeliveryDecoder struct {
	allowedMessageTypes         map[string]struct{}
	messageTypes                map[string]reflect.Type
	contentTypes                map[string]Deserializer
	ignoreUnknownMessageTypes   bool
	ignoreUnknownContentTypes   bool
	ignoreDeserializationErrors bool
	monitor                     monitor
	logger                      logger
}

func newDeliveryDecoder(config configuration) DeliveryDecoder {
	return defaultDeliveryDecoder{
		allowedMessageTypes:         config.AllowedTypes,
		messageTypes:                config.ReadTypes,
		contentTypes:                config.Deserializers,
		ignoreUnknownMessageTypes:   config.IgnoreUnknownMessageTypes,
		ignoreUnknownContentTypes:   config.IgnoreUnknownContentTypes,
		ignoreDeserializationErrors: config.IgnoreDeserializationErrors,
		monitor:                     config.Monitor,
		logger:                      config.Logger,
	}
}

func (this defaultDeliveryDecoder) Decode(delivery *messaging.Delivery) error {
	if len(delivery.Payload) == 0 || delivery.Message != nil {
		return nil
	}

	instanceType, found := this.messageTypes[delivery.MessageType]
	if !found {
		return this.handleUnknownMessageType(delivery)
	}

	_, found = this.allowedMessageTypes[delivery.MessageType]
	if !found && len(this.allowedMessageTypes) > 0 {
		return ErrMessageTypeNotAllowed
	}

	deserializer, found := this.contentTypes[delivery.ContentType]
	if !found {
		return this.handleUnknownContentType(delivery)
	}

	pointer := reflect.New(instanceType)
	err := deserializer.Deserialize(delivery.Payload, pointer.Interface())
	if err != nil {
		return this.handleDeserializationError(delivery, err)
	}

	this.monitor.MessageDecoded(nil)
	delivery.Message = pointer.Elem().Interface()
	return nil
}
func (this defaultDeliveryDecoder) handleUnknownMessageType(delivery *messaging.Delivery) error {
	this.monitor.MessageDecoded(ErrMessageTypeNotFound)

	if this.ignoreUnknownMessageTypes {
		this.logger.Printf("[WARN] Ignoring unknown message of type [%s].", delivery.MessageType)
		return nil
	} else {
		this.logger.Printf("[WARN] Unable to decode message of type [%s].", delivery.MessageType)
		return wrapError(fmt.Errorf("%w: [%s]", ErrMessageTypeNotFound, delivery.MessageType))
	}
}
func (this defaultDeliveryDecoder) handleUnknownContentType(delivery *messaging.Delivery) error {
	this.monitor.MessageDecoded(ErrUnknownContentType)

	if this.ignoreUnknownContentTypes {
		this.logger.Printf("[WARN] Ignoring message with Content-Type [%s].", delivery.ContentType)
		return nil
	} else {
		this.logger.Printf("[WARN] Unable to decode message with Content-Type [%s].", delivery.ContentType)
		return wrapError(fmt.Errorf("%w: [%s]", ErrUnknownContentType, delivery.ContentType))
	}
}
func (this defaultDeliveryDecoder) handleDeserializationError(delivery *messaging.Delivery, err error) error {
	this.monitor.MessageDecoded(err)

	if this.ignoreDeserializationErrors {
		this.logger.Printf("[WARN] Ignoring deserialization error for message of type [%s]: %s", delivery.MessageType, err)
		return nil
	} else {
		this.logger.Printf("[WARN] Unable to deserialize message of type [%s]: %s", delivery.MessageType, err)
		return wrapError(err)
	}
}
func wrapError(err error) error {
	return fmt.Errorf("%w: %s", ErrSerializationFailure, err)
}
