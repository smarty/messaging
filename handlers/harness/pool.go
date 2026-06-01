package harness

import "sync"

func newT[T any]() *T { return new(T) }

type poolT[T any] struct{ pool *sync.Pool }

func newPoolT[T any](new func() T) *poolT[T] {
	return &poolT[T]{pool: &sync.Pool{New: func() any { return new() }}}
}
func (this *poolT[T]) Get() T  { return this.pool.Get().(T) }
func (this *poolT[T]) Put(t T) { this.pool.Put(t) }
