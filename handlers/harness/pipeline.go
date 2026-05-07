package harness

import (
	"context"
	"time"

	"github.com/smarty/messaging/v3"
)

func build(ctx context.Context, cfg configuration) (messaging.Handler, []messaging.Listener) {
	var (
		batches = make(chan *batch, cfg.BatchCapacity)
		work1   = make(chan *unitOfWork, cfg.BatchCapacity)
		work2   = make(chan *unitOfWork, cfg.BatchCapacity)
		work3   = make(chan *unitOfWork, cfg.BatchCapacity)
		work4   = make(chan *unitOfWork, cfg.BatchCapacity)
		work5   = make(chan *unitOfWork, cfg.BatchCapacity)
	)

	var (
		entry       = newEntrypoint(cfg.Monitor, batches)
		exec        = newExecution(cfg.Monitor, cfg.UnitSize, batches, work1, newRouter(cfg.Types...))
		serializers = newFanOut(serializerFactory(cfg.Monitor, cfg.Serializer), cfg.SerializerCount, work1, work2)
		persist     = newPersistence(ctx, cfg.Monitor, work2, work3, cfg.Writer, time.Sleep)
		complete    = newCompletion(work3, work4)
		bcast       = newBroadcast(ctx, cfg.Monitor, work4, work5, cfg.Dispatcher, time.Sleep)
		term        = newTerminal(work5)
	)

	var listeners []messaging.Listener
	listeners = append(listeners,
		entry,
		exec,
	)
	listeners = append(listeners, serializers...)
	listeners = append(listeners,
		persist,
		complete,
		bcast,
		term,
	)
	return entry, listeners
}

func serializerFactory(monitor Monitor, enc serializer) stationFactory {
	return func(in, out chan *unitOfWork) messaging.Listener {
		return newSerialization(monitor, enc, in, out)
	}
}
