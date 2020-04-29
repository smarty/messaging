package sqlmq

import (
	"context"
	"sync"
	"time"

	"github.com/smartystreets/messaging/v3"
)

type dispatchProcessor struct {
	ctx       context.Context
	shutdown  context.CancelFunc
	channel   chan messaging.Dispatch
	retryWait time.Duration
	store     messageStore
	sender    messaging.Writer

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
	}
}

func (this *dispatchProcessor) Listen() {
	defer this.cleanup()

	var waiter sync.WaitGroup
	waiter.Add(1)
	defer waiter.Wait()

	go func() {
		defer waiter.Done()

		for this.isAlive() {
			if this.readPending() {
				return // completed initialization
			}
			this.sleep()
		}
	}()

	for this.isAlive() {
		if !this.write() {
			this.sleep()
		}
	}
}

func (this *dispatchProcessor) readPending() bool {
	dispatches, err := this.store.Load(this.ctx, this.latestID)

	for _, dispatch := range dispatches {
		this.latestID = dispatch.MessageID
		// dispatch.Topic = dispatch.MessageType // storage should do this
		this.channel <- dispatch
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

		if err := this.store.Confirm(this.ctx, this.buffer); err != nil {
			return false
		}

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
		return false
	}

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
	ctx, _ := context.WithTimeout(this.ctx, this.retryWait)
	<-ctx.Done()
}
func (this *dispatchProcessor) cleanup() {
	close(this.channel)
	if this.sender != nil {
		_ = this.sender.Close()
		this.sender = nil
	}
}

func (this *dispatchProcessor) Close() error {
	this.shutdown()
	return nil
}
