package sqlmq

import (
	"context"
	"sync"
	"time"

	"github.com/smarty/messaging/v4"
)

type dispatchProcessor struct {
	ctx       context.Context
	shutdown  context.CancelFunc
	channel   chan messaging.Dispatch
	retryWait time.Duration
	store     messageStore
	sender    messaging.Writer
	logger    logger
	monitor   monitor

	buffer   []messaging.Dispatch
	latestID uint64
	sent     bool
}

func newDispatchProcessor(config configuration) messaging.ListenCloser {
	ctx, shutdown := context.WithCancel(config.Context)
	return &dispatchProcessor{
		ctx:       ctx,
		shutdown:  shutdown,
		channel:   config.Channel,
		retryWait: config.Sleep,
		store:     config.MessageStore,
		sender:    config.Sender,
		logger:    config.Logger,
		monitor:   config.Monitor,
	}
}

func (this *dispatchProcessor) Listen() {
	defer this.cleanup()

	var waiter sync.WaitGroup
	defer waiter.Wait()

	waiter.Add(2)
	go this.listenInitialize(&waiter)
	go this.listenProcess(&waiter)
}
func (this *dispatchProcessor) listenInitialize(waiter *sync.WaitGroup) {
	defer waiter.Done()
	for this.isAlive() && !this.readPending() {
		this.sleep()
	}
}
func (this *dispatchProcessor) listenProcess(waiter *sync.WaitGroup) {
	defer waiter.Done()
	for this.isAlive() && !this.write() {
		this.sleep()
	}
}

func (this *dispatchProcessor) readPending() bool {
	dispatches, err := this.store.Load(this.ctx, this.latestID)
	if len(dispatches) > 0 {
		this.logger.Printf("[INFO] Startup recovery found [%d] undispatched message(s) in durable storage.", len(dispatches))
	}

	for _, dispatch := range dispatches {
		this.latestID = dispatch.MessageID
		select {
		case this.channel <- dispatch:
		case <-this.ctx.Done():
			return false // shutting down; the rows stay durable for the next startup
		}
	}

	if err != nil {
		this.logger.Printf("[WARN] Unable to load persisted messages from durable storage [%s].", err)
	}

	return err == nil
}

func (this *dispatchProcessor) write() bool {
	for {
		if !this.fillEmptyBuffer() {
			return false
		}

		if !this.writeBufferToSender() {
			return false
		}

		confirmed, err := this.store.Confirm(this.ctx, this.buffer)
		if err != nil {
			this.logger.Printf("[WARN] Unable to mark messages as dispatched in durable storage [%s].", err)
			return false
		}
		if confirmed != len(this.buffer) {
			this.logger.Printf("[WARN] Confirmed [%d] of [%d] published message(s) in durable storage. Another instance may have published the rest, or MessageIDs are out of step with the table (compare AutoincrementStride with auto_increment_increment).",
				confirmed, len(this.buffer))
		}

		this.monitor.MessageConfirmed(confirmed)
		this.clearBuffer()
	}
}
func (this *dispatchProcessor) fillEmptyBuffer() bool {
	if len(this.buffer) > 0 {
		return true // buffer hasn't yet been flushed, it's not empty and needs to be handled
	}

	select {
	case dispatch := <-this.channel:
		this.buffer = append(this.buffer, dispatch)

		length := len(this.channel)
		for i := 0; i < length; i++ {
			this.buffer = append(this.buffer, <-this.channel)
		}

		return true
	case <-this.ctx.Done():
		return false
	}
}
func (this *dispatchProcessor) writeBufferToSender() bool {
	if this.sent {
		return true
	}

	if _, err := this.sender.Write(this.ctx, this.buffer...); err != nil {
		this.logger.Printf("[WARN] Unable to publish [%d] message(s) to the transport [%s]; retrying in [%s].", len(this.buffer), err, this.retryWait)
		return false
	}

	this.monitor.MessagePublished(len(this.buffer))
	this.sent = true
	return true
}
func (this *dispatchProcessor) clearBuffer() {
	this.sent = false

	for i := 0; i < len(this.buffer); i++ {
		this.buffer[i] = messaging.Dispatch{} // clear it out to avoid a memory leak
	}

	this.buffer = this.buffer[0:0]
}

func (this *dispatchProcessor) isAlive() bool {
	select {
	case <-this.ctx.Done():
		return false
	default:
		return true
	}
}
func (this *dispatchProcessor) sleep() {
	ctx, cancel := context.WithTimeout(this.ctx, this.retryWait)
	defer cancel()
	<-ctx.Done()
}
func (this *dispatchProcessor) cleanup() {
	// The channel is deliberately left open. Handlers that committed SQL just
	// before shutdown and deferred handoff goroutines may still send on it;
	// a send on a closed channel panics even inside a select.
	if this.sender != nil {
		_ = this.sender.Close()
		this.sender = nil
	}
}

func (this *dispatchProcessor) Close() error {
	this.shutdown()
	return nil
}
