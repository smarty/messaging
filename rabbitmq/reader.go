package rabbitmq

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"sync"

	"github.com/smartystreets/messaging/v3"
	"github.com/smartystreets/messaging/v3/rabbitmq/adapter"
	"github.com/streadway/amqp"
)

type defaultReader struct {
	streams []io.Closer
	inner   adapter.Channel
	config  configuration
	mutex   sync.Mutex
	counter uint64
	logger  messaging.Logger

	hasExclusiveStream bool
}

func newReader(inner adapter.Channel, config configuration) messaging.Reader {
	return &defaultReader{inner: inner, config: config, logger: config.Logger}
}
func (this *defaultReader) Stream(_ context.Context, settings messaging.StreamConfig) (messaging.Stream, error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.hasExclusiveStream {
		return nil, ErrAlreadyExclusive
	}
	if settings.ExclusiveStream && len(this.streams) > 0 {
		return nil, ErrMultipleStreams
	}

	if err := this.establishTopology(settings); err != nil {
		_ = this.inner.Close()
		return nil, this.tryPanic(err)
	}

	if err := this.inner.BufferCapacity(settings.BufferCapacity); err != nil {
		this.logger.Printf("[WARN] Unable to set channel buffer size [%s].", err)
		_ = this.inner.Close()
		return nil, err
	}

	streamID := strconv.FormatUint(this.counter, 10)
	deliveries, err := this.inner.Consume(streamID, settings.StreamName)
	if err != nil {
		this.logger.Printf("[WARN] Unable to open consumer on channel [%s].", err)
		_ = this.inner.Close()
		return nil, err
	}

	stream := newStream(this.inner, deliveries, streamID, settings.ExclusiveStream, this.config)
	this.counter++
	this.streams = append(this.streams, stream)
	this.hasExclusiveStream = this.hasExclusiveStream || settings.ExclusiveStream
	return stream, nil
}
func (this *defaultReader) establishTopology(config messaging.StreamConfig) error {
	if !config.EstablishTopology {
		return nil
	}

	if err := this.inner.DeclareQueue(config.StreamName); err != nil {
		this.logger.Printf("[WARN] Unable to establish topology, queue declaration failed [%s].", err)
		return err
	}

	for _, topic := range config.Topics {
		if err := this.inner.DeclareExchange(topic); err != nil {
			this.logger.Printf("[WARN] Unable to establish topology, exchange declaration failed [%s].", err)
			return err
		}
		if err := this.inner.BindQueue(config.StreamName, topic); err != nil {
			this.logger.Printf("[WARN] Unable to establish topology, queue binding failed [%s].", err)
			return err
		}
	}

	return nil
}

func (this *defaultReader) tryPanic(err error) error {
	if err == nil || !this.config.TopologyFailurePanic {
		return err
	}

	if brokerError, ok := err.(*amqp.Error); ok && brokerError.Code == http.StatusNotAcceptable {
		panic(err)
	}

	return err
}

func (this *defaultReader) Close() error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	for i, stream := range this.streams {
		this.streams[i] = nil
		_ = stream.Close()
	}

	this.streams = this.streams[0:0]
	return this.inner.Close()
}
