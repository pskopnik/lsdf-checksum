package scheduler

import (
	"context"
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

	orderBook orderBook

	interval time.Duration
}

func New(config *Config) *Scheduler {
	return &Scheduler{
		Config:    config,
		orderBook: *newOrderBook(),
	}
}

func (q *Scheduler) Start(ctx context.Context) {
	q.fieldLogger = q.Config.Logger.WithFields(log.Fields{
		"namespace": q.Config.Namespace,
		"jobname":   q.Config.JobName,
		"component": "scheduler.QueueScheduler",
	})

	q.tomb, _ = tomb.WithContext(ctx)
	q.enqueuer = work.NewEnqueuer(q.Config.Namespace, q.Config.Pool)
	q.client = work.NewClient(q.Config.Namespace, q.Config.Pool)

	q.fieldLogger.Info("Performing scheduling controller initialisation")
	q.Config.Controller.Init(q)

	q.tomb.Go(q.run)
}

func (q *Scheduler) SignalStop() {
	q.tomb.Kill(lifecycle.ErrStopSignalled)
}

func (q *Scheduler) Wait() error {
	return q.tomb.Wait()
}

func (q *Scheduler) Dead() <-chan struct{} {
	return q.tomb.Dead()
}

func (q *Scheduler) Err() error {
	return q.tomb.Err()
}

func (q *Scheduler) run() error {
	dying := q.tomb.Dying()
	timer := time.NewTimer(time.Duration(0))

	q.fieldLogger.Info("Starting scheduling loop")

	// Exhaust timer
	if !timer.Stop() {
		<-timer.C
	}
L:
	for {
		timer.Reset(q.interval)

		select {
		case <-timer.C:
			q.fieldLogger.Debug("Calling schedule")
			q.Config.Controller.Schedule()
		case <-dying:
			// Exhaust timer
			if !timer.Stop() {
				<-timer.C
			}

			break L
		}
	}

	q.fieldLogger.WithField("action", "stopping").Info("Finished scheduling loop")

	return nil
}

// This method is part of the lower facing API (towards Controller).
func (q *Scheduler) GetEnqueued() uint64 {
	return q.orderBook.Stats().Fulfilled
}

// This method is part of the lower facing API (towards Controller).
func (q *Scheduler) SetInterval(interval time.Duration) {
	q.fieldLogger.WithField("interval", interval).Debug("Setting interval")
	q.interval = interval
}

// This method is part of the lower facing API (towards Controller).
func (q *Scheduler) GetQueueInfo() (count int64, latency int64, err error) {
	queues, err := q.client.Queues()
	if err != nil {
		return 0, 0, err
	}

	for _, queue := range queues {
		if queue.JobName == q.Config.JobName {
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
func (q *Scheduler) GetWorkerNum() (int, error) {
	heartbeats, err := q.client.WorkerPoolHeartbeats()
	if err != nil {
		return 0, err
	}

	workerNum := 0
	for _, heartbeat := range heartbeats {
		found := false
		for _, jobName := range heartbeat.JobNames {
			if jobName == q.Config.JobName {
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

// GetWorkerPoolNum returns the number of worker pool's currently registered
// with the queueing system.
// Each worker pool may have multiple workers, see GetWorkerNum() to retrieve
// the total concurrency.
//
// This method is part of the lower facing API (towards Controller).
func (q *Scheduler) GetWorkerPoolNum() (int, error) {
	heartbeats, err := q.client.WorkerPoolHeartbeats()
	if err != nil {
		return 0, err
	}

	return len(heartbeats), nil
}

// This method is part of the lower facing API (towards Controller).
func (q *Scheduler) RequestProduction(n uint) {
	q.fieldLogger.WithField("n", n).Debug("Requesting production")

	q.orderBook.Add(n)
}

func (q *Scheduler) RequestProductionUntilThreshold(threshold uint) {
	q.fieldLogger.WithField("threshold", threshold).Debug("Requesting production until threshold")

	q.orderBook.AddUntilThreshold(threshold)
}

// AcquireOrder acquires a production order from the internal order book of
// the Scheduler.
// The caller must fulfill the order by calling [ProductionOrder.Enqueue]
// [ProductionOrder.Total] times. The order will contain a maximum of max
// items.
//
// This method is part of the upper facing API (towards Producer).
func (q *Scheduler) AcquireOrder(ctx context.Context, max uint) (ProductionOrder, error) {
	order, err := q.orderBook.AcquireOrder(ctx, max)
	if err != nil {
		return ProductionOrder{}, err
	}

	return ProductionOrder{
		order:     order,
		scheduler: q,
	}, nil
}

type SchedulerStats struct {
	OrdersInQueue    uint64
	OrdersInProgress uint64
	JobsRequested    uint64
	JobsOrdered      uint64
	JobsEnqueued     uint64
}

func (q *Scheduler) Stats() SchedulerStats {
	stats := q.orderBook.Stats()
	return SchedulerStats{
		OrdersInQueue:    stats.InQueue,
		OrdersInProgress: stats.InProgress,
		JobsRequested:    stats.Requested,
		JobsOrdered:      stats.Ordered,
		JobsEnqueued:     stats.Fulfilled,
	}
}

func (q *Scheduler) enqueue(payload workqueue.JobPayload) (*work.Job, error) {
	args := make(map[string]interface{})

	err := payload.ToJobArgs(args)
	if err != nil {
		return nil, err
	}

	return q.enqueuer.Enqueue(q.Config.JobName, args)
}
