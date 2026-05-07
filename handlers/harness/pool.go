package harness

import "sync"

func newT[T any]() *T { return new(T) }

type poolT[T any] struct{ pool *sync.Pool }

func newPoolT[T any](new func() T) *poolT[T] {
	return &poolT[T]{pool: &sync.Pool{New: func() any { return new() }}}
}
func (p *poolT[T]) Get() T  { return p.pool.Get().(T) }
func (p *poolT[T]) Put(t T) { p.pool.Put(t) }
