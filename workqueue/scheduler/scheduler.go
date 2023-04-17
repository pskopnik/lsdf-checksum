package scheduler

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/apex/log"
	"github.com/gocraft/work"
	"gopkg.in/tomb.v2"

	"git.scc.kit.edu/sdm/lsdf-checksum/internal/lifecycle"
	"git.scc.kit.edu/sdm/lsdf-checksum/workqueue"
)

type ControllerScheduler interface {
	SetInterval(interval time.Duration)
	GetQueue() workqueue.QueueQuerier
	RequestProduction(n uint)
	RequestProductionUntilThreshold(threshold uint)
	Stats() SchedulerStats
}

type Controller interface {
	// Init is called once: When the Scheduler is starting.
	Init(scheduler ControllerScheduler)
	// Schedule is called by Scheduler with a frequency corresponding to
	// its interval value.
	// Schedule is never called concurrently, the next call to schedule takes
	// place `intv` time after the previous call completed.
	// RequestProduction() is the means of scheduling for the Controller.
	// During Schedule, the Controller should request the enqueueing of new
	// tasks by calling RequestProduction() on the Scheduler.
	Schedule()
}

type ProductionOrder[T workqueue.JobPayload] struct {
	order     orderBookOrder
	scheduler *Scheduler[T]
}

func (p *ProductionOrder[T]) Total() int {
	return p.order.Total()
}

func (p *ProductionOrder[T]) Remaining() int {
	return p.order.Remaining()
}

func (p *ProductionOrder[T]) Fulfilled() int {
	return p.order.Fulfilled()
}

func (p *ProductionOrder[T]) Enqueue(payload T) (*work.Job, error) {
	job, err := p.scheduler.enqueue(payload)
	if err != nil {
		return job, err
	}

	p.order.Fulfill(1)
	return job, nil
}

type Config struct {
	Controller Controller
	Logger     log.Interface
}

var _ ControllerScheduler = &Scheduler[*workqueue.WorkPack]{}

type Scheduler[T workqueue.JobPayload] struct {
	config Config

	tomb *tomb.Tomb

	queue       *workqueue.QueueClient[T]
	fieldLogger log.Interface

	orderBook          orderBook
	queueObservedEmpty atomic.Uint64

	interval time.Duration
}

func New[T workqueue.JobPayload](queue *workqueue.QueueClient[T], config Config) *Scheduler[T] {
	return &Scheduler[T]{
		config: config,
		queue: queue,
		orderBook: *newOrderBook(),
	}
}

func (s *Scheduler[T]) Start(ctx context.Context) {
	s.fieldLogger = s.config.Logger.WithFields(log.Fields{
		"queue":     s.queue.Name(),
		"component": "scheduler.QueueScheduler",
	})

	s.tomb, _ = tomb.WithContext(ctx)

	s.fieldLogger.Info("Performing scheduling controller initialisation")
	s.config.Controller.Init(s)

	s.tomb.Go(s.run)
}

func (s *Scheduler[T]) SignalStop() {
	s.tomb.Kill(lifecycle.ErrStopSignalled)
}

func (s *Scheduler[T]) Wait() error {
	return s.tomb.Wait()
}

func (s *Scheduler[T]) Dead() <-chan struct{} {
	return s.tomb.Dead()
}

func (s *Scheduler[T]) Err() error {
	return s.tomb.Err()
}

func (s *Scheduler[T]) run() error {
	dying := s.tomb.Dying()
	timer := time.NewTimer(time.Duration(0))

	s.fieldLogger.Info("Starting scheduling loop")

	// Exhaust timer
	if !timer.Stop() {
		<-timer.C
	}
L:
	for {
		timer.Reset(s.interval)

		select {
		case <-timer.C:
			s.fieldLogger.Debug("Calling schedule")
			s.config.Controller.Schedule()
		case <-dying:
			// Exhaust timer
			if !timer.Stop() {
				<-timer.C
			}

			break L
		}
	}

	s.fieldLogger.WithField("action", "stopping").Info("Finished scheduling loop")

	return nil
}

// This method is part of the lower facing API (towards Controller).
func (s *Scheduler[T]) SetInterval(interval time.Duration) {
	s.fieldLogger.WithField("interval", interval).Debug("Setting interval")
	s.interval = interval
}

// This method is part of the lower facing API (towards Controller).
func (s *Scheduler[T]) GetQueue() workqueue.QueueQuerier {
	return s.queue
}

// This method is part of the lower facing API (towards Controller).
func (s *Scheduler[T]) RequestProduction(n uint) {
	s.fieldLogger.WithField("n", n).Debug("Requesting production")

	s.orderBook.Add(n)
}

// This method is part of the lower facing API (towards Controller).
func (s *Scheduler[T]) RequestProductionUntilThreshold(threshold uint) {
	s.fieldLogger.WithField("threshold", threshold).Debug("Requesting production until threshold")

	s.orderBook.AddUntilThreshold(threshold)
}

// AcquireOrder acquires a production order from the internal order book of
// the Scheduler.
// The caller must fulfill the order by calling [ProductionOrder.Enqueue]
// [ProductionOrder.Total] times. The order will contain a maximum of max
// items.
//
// This method is part of the upper facing API (towards Producer).
func (s *Scheduler[T]) AcquireOrder(ctx context.Context, max uint) (ProductionOrder[T], error) {
	order, err := s.orderBook.AcquireOrder(ctx, max)
	if err != nil {
		return ProductionOrder[T]{}, err
	}

	return ProductionOrder[T]{
		order:     order,
		scheduler: s,
	}, nil
}

type SchedulerStats struct {
	OrdersInQueue      uint64
	OrdersInProgress   uint64
	JobsRequested      uint64
	JobsOrdered        uint64
	JobsEnqueued       uint64
	QueueObservedEmpty uint64
}

func (s *Scheduler[T]) Stats() SchedulerStats {
	stats := s.orderBook.Stats()
	return SchedulerStats{
		OrdersInQueue:      stats.InQueue,
		OrdersInProgress:   stats.InProgress,
		JobsRequested:      stats.Requested,
		JobsOrdered:        stats.Ordered,
		JobsEnqueued:       stats.Fulfilled,
		QueueObservedEmpty: s.queueObservedEmpty.Load(),
	}
}

func (s *Scheduler[T]) enqueue(payload T) (*work.Job, error) {
	job, err := s.queue.Enqueuer().Enqueue(payload)
	if err != nil {
		return nil, err
	}

	info, err := s.queue.GetQueueInfo()
	if err != nil {
		return nil, err
	}
	if info.QueuedJobs <= 1 {
		s.queueObservedEmpty.Add(1)
	}

	return job, nil
}
