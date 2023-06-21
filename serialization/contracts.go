package serialization

import (
	"errors"

	"github.com/smarty/messaging/v4"
)

type Serializer interface {
	ContentType() string
	Serialize(instance interface{}) ([]byte, error)
}
type Deserializer interface {
	ContentType() string
	Deserialize(source []byte, instance interface{}) error
}

type DeliveryDecoder interface {
	Decode(*messaging.Delivery) error
}
type DispatchEncoder interface {
	Encode(*messaging.Dispatch) error
}

type monitor interface {
	MessageEncoded(error)
	MessageDecoded(error)
}
type logger interface {
	Printf(format string, args ...interface{})
}

var (
	ErrMessageTypeNotAllowed       = errors.New("message type not allowed") // for topics that have lots of types we don't care about
	ErrSerializationFailure        = errors.New("serialization failure")
	ErrUnknownContentType          = errors.New("the content type provided was not understood")
	ErrMessageTypeNotFound         = errors.New("the message type provided was not understood")
	ErrDeserializeMalformedPayload = errors.New("the payload provided was not understood by the deserializer")
	ErrSerializeUnsupportedType    = errors.New("the type provided cannot be serialized")
)
