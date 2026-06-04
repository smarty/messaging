package harness

import (
	"context"

	"github.com/smarty/messaging/v3"
)

func build(ctx context.Context, config configuration) (forHTTP *httpAdapter, forMQ messaging.Handler, listeners []messaging.Listener) {
	var (
		batches = make(chan *batch, config.burstCapacity)
		work1   = make(chan *unitOfWork, config.pipelineBufferCapacity)
		work2   = make(chan *unitOfWork, config.pipelineBufferCapacity)
		work3   = make(chan *unitOfWork, config.pipelineBufferCapacity)
		work4   = make(chan *unitOfWork, config.pipelineBufferCapacity)
		work5   = make(chan *unitOfWork, config.pipelineBufferCapacity)
	)

	var (
		entrypoint  = newEntrypoint(config.monitor, batches, config.shedThreshold)
		executor    = newExecution(config.monitor, config.executionUnitSize, batches, work1, newRouter(config.types...))
		serializers = newFanOut(serializationFactory(config.monitor, config.serializer), config.serializerCount, config.pipelineBufferCapacity, work1, work2)
		persistence = newPersistence(ctx, config.monitor, work2, work3, config.writer, wait)
		completion  = newCompletion(work3, work4)
		broadcast   = newBroadcast(ctx, config.monitor, work4, work5, config.dispatcher, wait)
		terminal    = newTerminal(work5)
	)

	listeners = append(listeners,
		entrypoint,
		executor,
	)
	listeners = append(listeners, serializers...)
	listeners = append(listeners,
		persistence,
		completion,
		broadcast,
		terminal,
	)
	return newHTTPAdapter(entrypoint), entrypoint, listeners
}

func serializationFactory(monitor Monitor, enc serializer) stationFactory {
	return func(in, out chan *unitOfWork) messaging.Listener {
		return newSerialization(monitor, enc, in, out)
	}
}
