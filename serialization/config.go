package serialization

import (
	"reflect"

	"github.com/smartystreets/messaging/v3"
)

func New(connector messaging.Connector, options ...option) messaging.Connector {
	config := configuration{
		Connector:     connector,
		Deserializers: make(map[string]Deserializer),
	}
	Options.apply(options...)(&config)
	return newConnector(config)
}

type configuration struct {
	WriteTypes map[reflect.Type]string
	ReadTypes  map[string]reflect.Type

	IgnoreUnknownMessageTypes   bool
	IgnoreUnknownContentTypes   bool
	IgnoreDeserializationErrors bool

	TopicFromMessageType bool

	Deserializers map[string]Deserializer
	Serializer    Serializer

	Logger  logger
	Monitor monitor

	Connector messaging.Connector
	Encoder   DispatchEncoder
	Decoder   DeliveryDecoder
}

var Options singleton

type singleton struct{}
type option func(*configuration)

func (singleton) Serializer(value Serializer) option {
	return func(this *configuration) { this.Serializer = value }
}
func (singleton) AddDeserializer(value Deserializer, contentTypes ...string) option {
	return func(this *configuration) {
		for _, contentType := range contentTypes {
			this.Deserializers[contentType] = value
		}
	}
}
func (singleton) Decoder(value DeliveryDecoder) option {
	return func(this *configuration) { this.Decoder = value }
}
func (singleton) Encoder(value DispatchEncoder) option {
	return func(this *configuration) { this.Encoder = value }
}
func (singleton) ReadTypes(value map[string]reflect.Type) option {
	return func(this *configuration) { this.ReadTypes = value }
}
func (singleton) WriteTypes(value map[reflect.Type]string) option {
	return func(this *configuration) { this.WriteTypes = value }
}
func (singleton) IgnoreUnknownMessageTypes(value bool) option {
	return func(this *configuration) { this.IgnoreUnknownMessageTypes = value }
}
func (singleton) IgnoreUnknownContentTypes(value bool) option {
	return func(this *configuration) { this.IgnoreUnknownContentTypes = value }
}
func (singleton) IgnoreDeserializationErrors(value bool) option {
	return func(this *configuration) { this.IgnoreDeserializationErrors = value }
}
func (singleton) TopicFromMessageType(value bool) option {
	return func(this *configuration) { this.TopicFromMessageType = value }
}
func (singleton) Logger(value logger) option {
	return func(this *configuration) { this.Logger = value }
}
func (singleton) Monitor(value monitor) option {
	return func(this *configuration) { this.Monitor = value }
}

func (singleton) apply(options ...option) option {
	return func(this *configuration) {
		for _, option := range Options.defaults(options...) {
			option(this)
		}

		if this.Decoder == nil {
			this.Decoder = newDeliveryDecoder(*this)
		}
		if this.Encoder == nil {
			this.Encoder = newDispatchEncoder(*this)
		}
	}
}
func (singleton) defaults(options ...option) []option {
	var defaultSerializer = defaultSerializer{}
	var defaultLogger = nop{}
	var defaultMonitor = nop{}
	const emptyContentType = ""

	return append([]option{
		Options.Serializer(defaultSerializer),
		Options.AddDeserializer(defaultSerializer, defaultSerializer.ContentType(), emptyContentType),
		Options.IgnoreUnknownMessageTypes(false),
		Options.IgnoreUnknownContentTypes(false),
		Options.IgnoreDeserializationErrors(false),
		Options.TopicFromMessageType(true),
		Options.Logger(defaultLogger),
		Options.Monitor(defaultMonitor),
	}, options...)
}

type nop struct{}

func (nop) Printf(_ string, _ ...interface{}) {}

func (nop) MessageEncoded(_ error) {}
func (nop) MessageDecoded(_ error) {}
