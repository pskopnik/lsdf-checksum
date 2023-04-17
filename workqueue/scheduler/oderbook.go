package scheduler

import (
	"context"
	"sync/atomic"
)

type orderBookOrder struct {
	total     int
	fulfilled int
	book     *orderBook
}

func (o *orderBookOrder) Total() int {
	return o.total
}

func (o *orderBookOrder) Remaining() int {
	return o.total - o.fulfilled
}

func (o *orderBookOrder) Fulfilled() int {
	return o.fulfilled
}

func (o *orderBookOrder) Fulfill(n int) {
	o.fulfilled += n
	o.book.fulfilled.Add(uint64(n))
	o.book.inProgress.Add(uint64(-n))
}

// orderBook coordinates fulfillment of counted orders between components.
// Add methods add new orders to the queue ("request"), AcquireOrder takes on
// order for fulfillment ("ordered") and Fulfill on the returned order signals
// fulfillment of the order ("fulfillment").
// orderBook is comparable to a chan struct{} with an unbounded buffer and the
// cability to write/read an arbitrary number of entries.
// It also represents a semaphore with extended semantic.
type orderBook struct {
	// inQueue is the count of fresh orders waiting to be acquired.
	// Whenever changing this to a value > 0, a notify has to be triggered.
	inQueue atomic.Uint64
	// inProgress is the count of orders currently held by a caller but not
	// yet fulfilled.
	// It is always incremented after decrementing inQueue.
	inProgress atomic.Uint64

	// notifyC is a one-element-buffered chan to wait on when acquiring an
	// order.
	// It must always have an element waiting if inQueue > 0 (if there are no
	// ongoing method calls).
	notifyC chan struct{}

	// Stats counters

	requested atomic.Uint64
	ordered   atomic.Uint64
	fulfilled atomic.Uint64
}

func newOrderBook() *orderBook {
	return &orderBook{
		notifyC: make(chan struct{}, 1),
	}
}

func (o *orderBook) Add(n uint) {
	if n == 0 {
		return
	}
	o.inQueue.Add(uint64(n))
	o.requested.Add(uint64(n))
	o.notify()
}

func (o *orderBook) AddUntilThreshold(threshold uint) {
	for {
		// Loading and using inProgress is an optimistic approach.
		// inQueue+inProgress is subject to race conditions, but will always
		// be less equal to its true value.
		inProgress := o.inProgress.Load()
		inQueue := o.inQueue.Load()
		if inQueue+inProgress >= uint64(threshold) {
			break
		}
		if o.inQueue.CompareAndSwap(inQueue, uint64(threshold)-inProgress) {
			o.requested.Add(uint64(threshold) - inQueue)
			o.notify()
			break
		}
	}
}

func (o *orderBook) notify() {
	select {
	case o.notifyC <- struct{}{}:
	default:
	}
}

func (o *orderBook) AcquireOrder(ctx context.Context, max uint) (orderBookOrder, error) {
	// FUTR: min would also make sense, but should be coupled with a timeout
	//       to control latency. Implementation could call this method.
	// FUTR: Could have a Close() method to signal that no more requests
	//       will arrive. By closing notifyC this would be immediately
	//       communicated to AcquireOrder (first wait for inQueue to reach 0).
	for {
		select {
		case <-ctx.Done():
			return orderBookOrder{}, ctx.Err()
		case <-o.notifyC:
		}

		for {
			inQueue := o.inQueue.Load()
			if inQueue == 0 {
				// listen on notify channel
				break
			}
			c := uint64(max)
			if c > inQueue || max == 0 {
				c = inQueue
			}
			newInQueue := inQueue - c
			if !o.inQueue.CompareAndSwap(inQueue, newInQueue) {
				// try again to load level
				continue
			}
			o.inProgress.Add(uint64(c))
			o.ordered.Add(uint64(c))
			if newInQueue > 0 {
				o.notify()
			}
			return orderBookOrder{
				total: int(c),
				book: o,
			}, nil
		}
	}
}

type orderBookStats struct {
	InQueue    uint64
	InProgress uint64
	Requested  uint64
	Ordered    uint64
	Fulfilled  uint64
}

// Stats returns statistics about the current state of the orderBook.
// The different metrics within the snapshot may not be consistent.
func (o *orderBook) Stats() orderBookStats {
	return orderBookStats{
		InQueue:    o.inQueue.Load(),
		InProgress: o.inProgress.Load(),
		Requested:  o.requested.Load(),
		Ordered:    o.ordered.Load(),
		Fulfilled:  o.fulfilled.Load(),
	}
}
