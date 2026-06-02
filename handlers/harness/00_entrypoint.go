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

func (this *entrypoint) prepare(messages ...any) (waiter *sync.WaitGroup, batch *batch) {
	waiter = this.waiters.Get()
	waiter.Add(1)
	batch = this.batches.Get()
	batch.messages = messages
	batch.complete = func() {
		waiter.Done()
		this.monitor.Track(batchComplete)
		this.batches.Put(batch)
	}
	return waiter, batch
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

func (this *entrypoint) Handle(_ context.Context, messages ...any) {
	this.lock.RLock()
	if this.closed {
		this.lock.RUnlock()
		return
	}

	waiter, item := this.prepare(messages...)
	this.work <- item
	this.lock.RUnlock()
	this.monitor.Track(batchInFlight)

	waiter.Wait()
	this.waiters.Put(waiter)
}

func (this *entrypoint) await(ctx context.Context, message any) {
	this.lock.RLock()
	if this.closed {
		this.lock.RUnlock()
		return
	}

	waiter, batch := this.prepare(message)

	select {
	case this.work <- batch:
		this.lock.RUnlock()
		this.monitor.Track(batchInFlight)
	case <-ctx.Done():
		this.lock.RUnlock()
		this.abandon(waiter, batch)
		this.waiters.Put(waiter) // safe: abandon() called Done() (count 0); no detached waiter.
		this.monitor.Track(callerDeparted)
		return
	}

	select {
	case <-this.waiterDone(waiter):
		this.waiters.Put(waiter) // safe: detached Wait() returned before done was closed (count 0).
	case <-ctx.Done():
		this.monitor.Track(callerDeparted)
		// Intentionally do NOT recycle the waiter here: complete() is still pending
		// and the detached waiterDone goroutine is still inside Wait(). Returning it
		// to the pool now would let a later prepare() Add(1) before this Wait()
		// returns -- documented sync.WaitGroup misuse. Get() already removed the
		// pool's reference, so declining to Put() leaves nothing pool-held; the
		// waiter is released to GC once complete() fires.
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

var (
	batchInFlight  BatchInFlight
	batchComplete  BatchComplete
	loadShed       LoadShed
	callerDeparted CallerDeparted
)
