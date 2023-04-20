// Package watcher contains a straightforward implementation of the "watch
// value for changes" pattern.
package watcher

import (
	"context"
	"sync"
	"sync/atomic"
)

type Publisher[T any] struct {
	value atomic.Value

	m          sync.RWMutex
	watchChans []chan struct{}
}

func (p *Publisher[T]) Publish(value T) {
	p.value.Store(value)

	p.m.RLock()
	for _, watcher := range p.watchChans {
		select {
		case watcher <- struct{}{}:
		default:
		}
	}
	p.m.RUnlock()
}

func (p *Publisher[T]) Get() T {
	return p.value.Load().(T)
}

func (p *Publisher[T]) Watch() *Watcher[T] {
	w := &Watcher[T]{
		p: p,
		c: make(chan struct{}),
	}

	p.m.Lock()
	p.watchChans = append(p.watchChans, w.c)
	p.m.Unlock()

	return w
}

func (p *Publisher[T]) unwatch(w *Watcher[T]) {
	p.m.Lock()
	for i, watcher := range p.watchChans {
		if watcher == w.c {
			p.watchChans[i] = p.watchChans[len(p.watchChans)-1]
			p.watchChans = p.watchChans[:len(p.watchChans)-1]
		}
	}
	p.m.Unlock()
}

type Watcher[T any] struct {
	p *Publisher[T]
	c chan struct{}
}

func (w *Watcher[T]) C() <-chan struct{} {
	return w.c
}

func (w *Watcher[T]) Get() T {
	return w.p.Get()
}

func (w *Watcher[T]) Wait(ctx context.Context) T {
	select {
	case <-w.C():
	case <-ctx.Done():
	}

	return w.Get()
}

func (w *Watcher[T]) Close() {
	w.p.unwatch(w)
}
