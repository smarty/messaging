package harness

import (
	"context"
	"sync"
)

type entrypoint struct {
	monitor Monitor
	waiters *poolT[*sync.WaitGroup]
	batches *poolT[*batch]
	work    chan *batch
	lock    *sync.RWMutex
	closed  bool
	done    chan struct{}
}

func newEntrypoint(monitor Monitor, work chan *batch) *entrypoint {
	return &entrypoint{
		monitor: monitor,
		waiters: newPoolT(newT[sync.WaitGroup]),
		batches: newPoolT(newT[batch]),
		work:    work,
		lock:    new(sync.RWMutex),
		done:    make(chan struct{}),
	}
}

func (this *entrypoint) Handle(ctx context.Context, messages ...any) {
	waiter := this.waiters.Get()
	defer this.waiters.Put(waiter)
	waiter.Add(1)
	defer waiter.Wait()

	item := this.batches.Get()
	item.ctx = ctx
	item.messages = messages
	item.complete = func() {
		waiter.Done()
		this.monitor.Track(batchComplete)
		this.batches.Put(item)
	}

	this.lock.RLock()
	if !this.closed {
		this.work <- item
		this.monitor.Track(batchInFlight)
	}
	this.lock.RUnlock()
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
