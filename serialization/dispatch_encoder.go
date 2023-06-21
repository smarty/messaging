package serialization

import (
	"fmt"
	"reflect"

	"github.com/smarty/messaging/v4"
)

type defaultDispatchEncoder struct {
	messageTypes         map[reflect.Type]string
	contentType          string
	topicFromMessageType bool
	serializer           Serializer
	monitor              monitor
	logger               logger
}

func newDispatchEncoder(config configuration) DispatchEncoder {
	return defaultDispatchEncoder{
		messageTypes:         config.WriteTypes,
		contentType:          config.Serializer.ContentType(),
		serializer:           config.Serializer,
		topicFromMessageType: config.TopicFromMessageType,
		monitor:              config.Monitor,
		logger:               config.Logger,
	}
}

func (this defaultDispatchEncoder) Encode(dispatch *messaging.Dispatch) error {
	if len(dispatch.Payload) > 0 || dispatch.Message == nil {
		return nil // already written or nothing to serialize
	}

	instanceType := reflect.TypeOf(dispatch.Message)
	messageType, found := this.messageTypes[reflect.TypeOf(dispatch.Message)]
	if !found {
		this.monitor.MessageEncoded(ErrMessageTypeNotFound)
		this.logger.Printf("[WARN] Unable to encode message of type [%s], message type not found.", reflect.TypeOf(dispatch.Message))
		return wrapError(fmt.Errorf("%w: [%s]", ErrMessageTypeNotFound, instanceType.Name()))
	}

	raw, err := this.serializer.Serialize(dispatch.Message)
	if err != nil {
		this.monitor.MessageEncoded(err)
		this.logger.Printf("[WARN] Unable to serialize message of type [%s]: %s", reflect.TypeOf(dispatch.Message), err)
		return wrapError(err)
	}

	this.monitor.MessageEncoded(nil)
	dispatch.ContentType = this.contentType
	dispatch.MessageType = messageType
	dispatch.Payload = raw

	if this.topicFromMessageType && dispatch.Topic == "" {
		dispatch.Topic = messageType
	}

	return nil
}
