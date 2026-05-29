package harness

import (
	"context"
	"sync"
)

type entrypoint struct {
	monitor       Monitor
	waiters       *poolT[*sync.WaitGroup]
	batches       *poolT[*batch]
	work          chan *batch
	lock          *sync.RWMutex
	closed        bool
	done          chan struct{}
	shedThreshold float64
}

func newEntrypoint(monitor Monitor, work chan *batch, shedThreshold float64) *entrypoint {
	return &entrypoint{
		monitor:       monitor,
		waiters:       newPoolT(newT[sync.WaitGroup]),
		batches:       newPoolT(newT[batch]),
		work:          work,
		lock:          new(sync.RWMutex),
		done:          make(chan struct{}),
		shedThreshold: shedThreshold,
	}
}

func (this *entrypoint) prepare(ctx context.Context, messages ...any) (waiter *sync.WaitGroup, item *batch) {
	waiter = this.waiters.Get()
	waiter.Add(1)
	item = this.batches.Get()
	item.ctx = ctx
	item.messages = messages
	item.complete = func() {
		waiter.Done()
		this.monitor.Track(batchComplete)
		this.batches.Put(item)
	}
	return waiter, item
}

func (this *entrypoint) abandon(waiter *sync.WaitGroup, item *batch) {
	waiter.Done()
	this.batches.Put(item)
}

func (this *entrypoint) waiterDone(waiter *sync.WaitGroup) (done chan struct{}) {
	done = make(chan struct{})
	go func() { waiter.Wait(); close(done) }()
	return done
}

func (this *entrypoint) Handle(ctx context.Context, messages ...any) {
	waiter, item := this.prepare(ctx, messages...)
	defer this.waiters.Put(waiter)

	this.lock.RLock()
	if this.closed {
		this.lock.RUnlock()
		this.abandon(waiter, item)
		return
	}
	this.work <- item
	this.monitor.Track(batchInFlight)
	this.lock.RUnlock()

	waiter.Wait()
}

func (this *entrypoint) await(ctx context.Context, message any) {
	waiter, item := this.prepare(ctx, message)
	defer this.waiters.Put(waiter)

	this.lock.RLock()
	if this.closed {
		this.lock.RUnlock()
		this.abandon(waiter, item)
		return
	}
	select {
	case this.work <- item:
		this.monitor.Track(batchInFlight)
		this.lock.RUnlock()
	case <-ctx.Done():
		this.lock.RUnlock()
		this.abandon(waiter, item)
		this.monitor.Track(callerDeparted)
		return
	}

	select {
	case <-this.waiterDone(waiter):
	case <-ctx.Done():
		this.monitor.Track(callerDeparted)
	}
}

func (this *entrypoint) admit() bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	if this.closed {
		return false
	}
	if float64(len(this.work))/float64(cap(this.work)) >= this.shedThreshold {
		this.monitor.Track(loadShed)
		return false
	}
	return true
}

// Listen blocks until Close is called so the entrypoint can be added as a dominoes listener.
// This guarantees Close is invoked during shutdown, which closes the work channel and lets the
// downstream pipeline stages drain naturally.
func (this *entrypoint) Listen() { <-this.done }

func (this *entrypoint) Close() error {
	this.lock.Lock()
	if !this.closed {
		close(this.work)
		close(this.done)
		this.closed = true
	}
	this.lock.Unlock()
	return nil
}

var batchInFlight BatchInFlight
var batchComplete BatchComplete
var loadShed LoadShed
var callerDeparted CallerDeparted
