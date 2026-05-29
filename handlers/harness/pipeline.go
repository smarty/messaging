package harness

import (
	"context"
	"time"

	"github.com/smarty/messaging/v3"
)

func build(ctx context.Context, config configuration) (messaging.Handler, []messaging.Listener) {
	var (
		batches = make(chan *batch, config.BatchCapacity)
		work1   = make(chan *unitOfWork, config.UnitCapacity)
		work2   = make(chan *unitOfWork, config.UnitCapacity)
		work3   = make(chan *unitOfWork, config.UnitCapacity)
		work4   = make(chan *unitOfWork, config.UnitCapacity)
		work5   = make(chan *unitOfWork, config.UnitCapacity)
	)

	var (
		entrypoint  = newEntrypoint(config.Monitor, batches, config.ShedThreshold)
		executor    = newExecution(config.Monitor, config.UnitSize, batches, work1, newRouter(config.Types...))
		serializers = newFanOut(serializationFactory(config.Monitor, config.Serializer), config.SerializerCount, config.UnitCapacity, work1, work2)
		persistence = newPersistence(ctx, config.Monitor, work2, work3, config.Writer, time.Sleep)
		completion  = newCompletion(work3, work4)
		broadcast   = newBroadcast(ctx, config.Monitor, work4, work5, config.Dispatcher, time.Sleep)
		terminal    = newTerminal(work5)
	)

	var listeners []messaging.Listener
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
	return entrypoint, listeners
}

func serializationFactory(monitor Monitor, enc serializer) stationFactory {
	return func(in, out chan *unitOfWork) messaging.Listener {
		return newSerialization(monitor, enc, in, out)
	}
}
