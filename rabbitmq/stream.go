package rabbitmq

import (
	"context"
	"io"
	"strconv"
	"sync"

	"github.com/smartystreets/messaging/v4"
	"github.com/smartystreets/messaging/v4/rabbitmq/adapter"
	"github.com/streadway/amqp"
)

type defaultStream struct {
	channel    adapter.Channel
	deliveries <-chan amqp.Delivery
	streamID   string
	streamName string
	batchAck   bool
	closer     sync.Once
	logger     logger
	monitor    monitor
}

func newStream(channel adapter.Channel, deliveries <-chan amqp.Delivery, id, name string, exclusive bool, config configuration) messaging.Stream {
	return &defaultStream{
		channel:    channel,
		deliveries: deliveries,
		streamID:   id,
		streamName: name,
		batchAck:   exclusive,
		logger:     config.Logger,
		monitor:    config.Monitor,
	}
}

func (this *defaultStream) Read(ctx context.Context, target *messaging.Delivery) error {
	select {
	case source, open := <-this.deliveries:
		return this.processDelivery(source, target, open)
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (this *defaultStream) processDelivery(source amqp.Delivery, target *messaging.Delivery, deliveryChannelOpen bool) error {
	if !deliveryChannelOpen {
		return io.EOF
	}

	target.Upstream = source
	target.DeliveryID = source.DeliveryTag
	target.SourceID = parseUint64(source.AppId)
	target.MessageID = parseUint64(source.MessageId)
	target.CorrelationID = parseUint64(source.CorrelationId)
	target.Timestamp = source.Timestamp
	target.Durable = source.DeliveryMode == amqp.Persistent
	target.Topic = this.streamName
	target.MessageType = source.Type
	target.ContentType = source.ContentType
	target.ContentEncoding = source.ContentEncoding
	target.Headers = source.Headers
	target.Payload = source.Body

	this.monitor.DeliveryReceived()
	return nil
}
func parseUint64(value string) uint64 {
	parsed, _ := strconv.ParseUint(value, 10, 64)
	return parsed
}

func (this *defaultStream) Acknowledge(ctx context.Context, deliveries ...messaging.Delivery) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	length := len(deliveries)
	if length > 1 && this.batchAck {
		deliveries = deliveries[length-1:] // only ack the last one
	}

	for _, delivery := range deliveries {
		if err := this.channel.Ack(delivery.DeliveryID, this.batchAck); err != nil {
			this.logger.Printf("[WARN] Unable to acknowledge delivery against underlying channel [%s].", err)
			this.monitor.DeliveryAcknowledged(uint16(length), err)
			return err
		}
	}

	this.monitor.DeliveryAcknowledged(uint16(length), nil)
	return nil
}

func (this *defaultStream) Close() (err error) {
	this.closer.Do(func() {
		err = this.channel.CancelConsumer(this.streamID)
	})
	return err
}
