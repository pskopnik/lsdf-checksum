package scheduler

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/apex/log"
	"github.com/gocraft/work"
	"github.com/gomodule/redigo/redis"
	"gopkg.in/tomb.v2"

	"git.scc.kit.edu/sdm/lsdf-checksum/internal/lifecycle"
	"git.scc.kit.edu/sdm/lsdf-checksum/workqueue"
)

type Controller interface {
	// Init is called once: When the Scheduler is starting.
	Init(scheduler *Scheduler)
	// Schedule is called by Scheduler with a frequency corresponding to
	// its interval value.
	// Schedule is never called concurrently, the next call to schedule takes
	// place `intv` time after the previous call completed.
	// RequestProduction() is the means of scheduling for the Controller.
	// During Schedule, the Controller should request the enqueueing of new
	// tasks by calling RequestProduction() on the Scheduler.
	Schedule()
}

type ProductionOrder struct {
	order     orderBookOrder
	scheduler *Scheduler
}

func (p *ProductionOrder) Total() int {
	return p.order.Total()
}

func (p *ProductionOrder) Remaining() int {
	return p.order.Remaining()
}

func (p *ProductionOrder) Fulfilled() int {
	return p.order.Fulfilled()
}

func (p *ProductionOrder) Enqueue(payload workqueue.JobPayload) (*work.Job, error) {
	job, err := p.scheduler.enqueue(payload)
	if err != nil {
		return job, err
	}

	p.order.Fulfill(1)
	return job, nil
}

//go:generate confions config Config

type Config struct {
	Namespace string
	JobName   string

	Pool   *redis.Pool   `yaml:"-"`
	Logger log.Interface `yaml:"-"`

	Controller Controller
}

var DefaultConfig = &Config{}

type Scheduler struct {
	// Config contains the configuration of the QueueScheduler.
	// Config must not be modified after Start() has been called.
	Config *Config

	tomb *tomb.Tomb

	enqueuer    *work.Enqueuer
	client      *work.Client
	fieldLogger log.Interface

	orderBook          orderBook
	queueObservedEmpty atomic.Uint64

	interval time.Duration
}

func New(config *Config) *Scheduler {
	return &Scheduler{
		Config:    config,
		orderBook: *newOrderBook(),
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	s.fieldLogger = s.Config.Logger.WithFields(log.Fields{
		"namespace": s.Config.Namespace,
		"jobname":   s.Config.JobName,
		"component": "scheduler.QueueScheduler",
	})

	s.tomb, _ = tomb.WithContext(ctx)
	s.enqueuer = work.NewEnqueuer(s.Config.Namespace, s.Config.Pool)
	s.client = work.NewClient(s.Config.Namespace, s.Config.Pool)

	s.fieldLogger.Info("Performing scheduling controller initialisation")
	s.Config.Controller.Init(s)

	s.tomb.Go(s.run)
}

func (s *Scheduler) SignalStop() {
	s.tomb.Kill(lifecycle.ErrStopSignalled)
}

func (s *Scheduler) Wait() error {
	return s.tomb.Wait()
}

func (s *Scheduler) Dead() <-chan struct{} {
	return s.tomb.Dead()
}

func (s *Scheduler) Err() error {
	return s.tomb.Err()
}

func (s *Scheduler) run() error {
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
			s.Config.Controller.Schedule()
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
func (s *Scheduler) SetInterval(interval time.Duration) {
	s.fieldLogger.WithField("interval", interval).Debug("Setting interval")
	s.interval = interval
}

// This method is part of the lower facing API (towards Controller).
func (s *Scheduler) GetQueueInfo() (count int64, latency int64, err error) {
	queues, err := s.client.Queues()
	if err != nil {
		return 0, 0, err
	}

	for _, queue := range queues {
		if queue.JobName == s.Config.JobName {
			return queue.Count, queue.Latency, nil
		}
	}

	// The queue has not been found, that means it has not been initialised
	// yet.
	return 0, 0, nil
}

// GetWorkerNum returns the number of (alive) workers currently registered
// with the queueing system.
// This is equal to the total currency of the queueing system.
//
// This method is part of the lower facing API (towards Controller).
func (s *Scheduler) GetWorkerNum() (int, error) {
	heartbeats, err := s.client.WorkerPoolHeartbeats()
	if err != nil {
		return 0, err
	}

	workerNum := 0
	for _, heartbeat := range heartbeats {
		found := false
		for _, jobName := range heartbeat.JobNames {
			if jobName == s.Config.JobName {
				found = true
				break
			}
		}
		if !found {
			continue
		}

		workerNum += int(heartbeat.Concurrency)
	}

	return workerNum, nil
}

// This method is part of the lower facing API (towards Controller).
func (s *Scheduler) RequestProduction(n uint) {
	s.fieldLogger.WithField("n", n).Debug("Requesting production")

	s.orderBook.Add(n)
}

// This method is part of the lower facing API (towards Controller).
func (s *Scheduler) RequestProductionUntilThreshold(threshold uint) {
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
func (s *Scheduler) AcquireOrder(ctx context.Context, max uint) (ProductionOrder, error) {
	order, err := s.orderBook.AcquireOrder(ctx, max)
	if err != nil {
		return ProductionOrder{}, err
	}

	return ProductionOrder{
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

func (s *Scheduler) Stats() SchedulerStats {
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

func (s *Scheduler) enqueue(payload workqueue.JobPayload) (*work.Job, error) {
	args := make(map[string]interface{})

	err := payload.ToJobArgs(args)
	if err != nil {
		return nil, err
	}

	job, err := s.enqueuer.Enqueue(s.Config.JobName, args)
	if err != nil {
		return nil, err
	}

	queueLength, _, err := s.GetQueueInfo()
	if err != nil {
		return nil, err
	}
	if queueLength <= 1 {
		s.queueObservedEmpty.Add(1)
	}

	return job, nil
}
