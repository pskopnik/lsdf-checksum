package scheduler

import (
	"context"
	"math"
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

//go:generate confions config EWMAControllerConfig

type EWMAControllerConfig struct {
	ConsumptionLifetime time.Duration

	MinThreshold       uint
	MinWorkerThreshold float64

	StartUpSteps          uint
	StartUpInterval       time.Duration
	StartUpWorkerTheshold uint

	MaintainingInterval        time.Duration
	MaintainingDeviationFactor float64
	MaintainingDeviationAlpha  float64
}

var EWMAControllerDefaultConfig = &EWMAControllerConfig{
	ConsumptionLifetime: 10 * time.Second,

	MinThreshold:       4,
	MinWorkerThreshold: 0.25,

	StartUpSteps:          1000,
	StartUpInterval:       10 * time.Millisecond,
	StartUpWorkerTheshold: 5,

	MaintainingInterval:        10 * time.Second,
	MaintainingDeviationFactor: 10,
	MaintainingDeviationAlpha:  0.1,
}

type ewmaControllerPhase int

// Constants related to EWMAController
const (
	espUninitialised ewmaControllerPhase = iota
	espStartUp
	espMaintaining
)

var _ Controller = &EWMAController{}

type EWMAController struct {
	Config *EWMAControllerConfig

	queueScheduler *Scheduler
	fieldLogger    log.Interface

	phase                   ewmaControllerPhase
	startUpStepCount        uint
	previousScheduling      time.Time
	previousQueueLength     uint
	previousEnqueuedCounter uint64
	threshold               uint
	// deviationEWMA is normalised to deviation (of consumption) per second
	deviationEWMA float64
	// consumptionEWMA is normalised to consumption per second
	consumptionEWMA float64
}

func NewEWMAController(config *EWMAControllerConfig) *EWMAController {
	return &EWMAController{
		Config: config,
	}
}

func (e *EWMAController) Init(queueScheduler *Scheduler) {
	e.queueScheduler = queueScheduler
	e.fieldLogger = queueScheduler.Config.Logger.WithFields(log.Fields{
		"component": "scheduler.EWMAController",
	})

	// Initialise state
	e.phase = espStartUp
	e.startUpStepCount = 0
	e.previousQueueLength = 0
	e.deviationEWMA = 0
	e.consumptionEWMA = 0

	// Initial scheduling
	e.scheduleInit()
}

func (e *EWMAController) scheduleInit() {
	e.queueScheduler.SetInterval(e.Config.StartUpInterval)
	workerNum, err := e.queueScheduler.GetWorkerNum()
	if err != nil {
		workerNum = 0
	}

	e.threshold = e.minLength(uint(workerNum)*e.Config.StartUpWorkerTheshold, uint(workerNum))
	e.fieldLogger.WithField("threshold", e.threshold).WithField("phase", "startup").Debug("Calculated initial threshold")

	queueLength, _, err := e.queueScheduler.GetQueueInfo()
	if err != nil {
		e.fieldLogger.WithError(err).WithField("action", "skipping").Debug("No scheduling possible, couldn't get queueLength")
		// No scheduling possible
		return
	}

	e.fieldLogger.WithFields(log.Fields{
		"queue_length": queueLength,
		"threshold":    e.threshold,
		"worker_num":   workerNum,
	}).Debug("Planning production request")
	if queueLength < int64(e.threshold) {
		e.queueScheduler.RequestProduction(e.threshold - uint(queueLength))
	}
}

func (e *EWMAController) Schedule() {
	now := time.Now()
	timePassed := now.Sub(e.previousScheduling)
	e.previousScheduling = now

	queueLength, _, err := e.queueScheduler.GetQueueInfo()
	if err != nil {
		e.fieldLogger.WithError(err).WithField("action", "skipping").
			Debug("No scheduling possible, couldn't get queueLength")
		// No scheduling possible
		return
	}
	exhausted := queueLength == 0
	enqueuedCounter := e.queueScheduler.GetEnqueued()
	enqueued := enqueuedCounter - e.previousEnqueuedCounter
	e.previousEnqueuedCounter = enqueuedCounter

	workerNum, err := e.queueScheduler.GetWorkerNum()
	if err != nil {
		// No scheduling possible
		return
	}

	var consumption uint
	if e.previousQueueLength+uint(enqueued) < uint(queueLength) {
		consumption = 0
	} else {
		consumption = e.previousQueueLength + uint(enqueued) - uint(queueLength)
	}

	deviation := float64(consumption) - normaliseDuration(timePassed, time.Second, e.consumptionEWMA)
	if deviation < 0 {
		deviation = -deviation
	}

	consumptionAlpha := float64(1.0) - math.Exp(-normaliseDuration(timePassed, e.Config.ConsumptionLifetime))
	e.consumptionEWMA = updateEwma(e.consumptionEWMA, consumptionAlpha, normaliseDuration(time.Second, timePassed, float64(consumption)))

	e.deviationEWMA = updateEwma(e.deviationEWMA, e.Config.MaintainingDeviationAlpha, normaliseDuration(time.Second, timePassed, deviation))

	e.fieldLogger.WithFields(log.Fields{
		"previous_queue_length": e.previousQueueLength,
		"queue_length":          queueLength,
		"exhausted":             exhausted,
		"enqueued":              enqueued,
		"worker_num":            workerNum,
		"consumption":           consumption,
		"deviation":             deviation,
		"consumption_alpha":     consumptionAlpha,
		"consumption_ewma":      e.consumptionEWMA,
		"deviation_alpha":       e.Config.MaintainingDeviationAlpha,
		"deviation_ewma":        e.deviationEWMA,
	}).Debug("Probed queue and calculated derivatives")

	e.previousQueueLength = uint(queueLength)

	if exhausted {
		prevThreshold := e.threshold
		if e.threshold == 0 {
			e.threshold = 1
		} else {
			e.threshold *= 2
		}
		e.fieldLogger.WithFields(log.Fields{
			"previous_threshold": prevThreshold,
			"threshold":          e.threshold,
		}).Debug("Exhausted, doubled threshold")
	} else {
		switch e.phase {
		case espStartUp:
			e.startUpStepCount++
			e.threshold = e.minLength(e.startupThreshold(uint(workerNum)), uint(workerNum))

			e.fieldLogger.WithFields(log.Fields{
				"threshold": e.threshold,
				"phase":     "startup",
			}).Debug("Calculated threshold")
			if e.startUpStepCount >= e.Config.StartUpSteps {
				e.phase = espMaintaining
				e.queueScheduler.SetInterval(e.Config.MaintainingInterval)

				e.threshold = e.minLength(e.maintainingThreshold(), uint(workerNum))
				e.fieldLogger.WithFields(log.Fields{
					"threshold": e.threshold,
					"phase":     "startup",
				}).Debug("Switching to maintaining phase")
			}
		case espMaintaining:
			e.threshold = e.minLength(e.maintainingThreshold(), uint(workerNum))
			e.fieldLogger.WithFields(log.Fields{
				"threshold": e.threshold,
				"phase":     "maintaining",
			}).Debug("Calculated threshold")
		default:
			e.threshold = e.minLength(0, uint(workerNum))
			e.fieldLogger.WithFields(log.Fields{
				"threshold": e.threshold,
				"phase":     "unknown",
			}).Debug("Calculated threshold")
		}
	}

	e.fieldLogger.WithFields(log.Fields{
		"queue_length": queueLength,
		"threshold":    e.threshold,
		"worker_num":   workerNum,
	}).Debug("Planning production request")
	if queueLength < int64(e.threshold) {
		e.queueScheduler.RequestProduction(e.threshold - uint(queueLength))
	}
}

// startupThreshold calculates the threshold during the startup phase using
// only the current number of workers and the StartUpWorkerTheshold config
// option.
func (e *EWMAController) startupThreshold(workerNum uint) uint {
	return uint(workerNum) * e.Config.StartUpWorkerTheshold
}

// maintainingThreshold calculates the threshold during the maintaining phase
// from the ewma fields which are tracked as part of the state.
func (e *EWMAController) maintainingThreshold() uint {
	expDeviation := normaliseDuration(e.Config.MaintainingInterval, time.Second, e.deviationEWMA)
	expConsumption := normaliseDuration(e.Config.MaintainingInterval, time.Second, e.consumptionEWMA)

	calculatedThreshold := math.Ceil(e.Config.MaintainingDeviationFactor*expDeviation + expConsumption)

	e.fieldLogger.WithFields(log.Fields{
		"exp_deviation":        expDeviation,
		"exp_consumption":      expConsumption,
		"calculated_threshold": calculatedThreshold,
		"uint_threshold":       uint(calculatedThreshold),
	}).Debug("Calculating maintaining threshold")

	return uint(calculatedThreshold)
}

// minLength calculates the minimum length of the queue based on any calculated
// length (from the scheduling algorithm) and the MinThreshold and
// MinWorkerThreshold configuration options.
//
// The maximum of all minimum lengths is returned.
func (e *EWMAController) minLength(calculatedLength uint, workerNum uint) uint {
	// Calculate minLength based on the MinWorkerThreshold config option and the workerNum
	minLength := uint(math.Ceil(e.Config.MinWorkerThreshold * float64(workerNum)))

	if e.Config.MinThreshold > minLength {
		minLength = e.Config.MinThreshold
	}

	if calculatedLength > minLength {
		minLength = calculatedLength
	}

	if minLength == 0 {
		minLength = 1
	}

	return minLength
}

// (1 - alpha) * ewma + alpha * value
func updateEwma(ewma, alpha, value float64) float64 {
	return (1-alpha)*ewma + alpha*value
}

// (duration / normalDuration) * factor_1 * factor_2 * ...
func normaliseDuration(duration, normalDuration time.Duration, factors ...float64) float64 {
	res := float64(duration.Nanoseconds()) / float64(normalDuration.Nanoseconds())

	for _, factor := range factors {
		res *= factor
	}

	return res
}
