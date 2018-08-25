package workqueue

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/gocraft/work"
	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
	"gopkg.in/tomb.v2"

	"git.scc.kit.edu/sdm/lsdf-checksum/internal/lifecycle"
)

type SchedulingController interface {
	// Init is called once: When the QueueScheduler is starting.
	Init(scheduler *QueueScheduler)
	// Schedule is called by QueueScheduler with a frequency corresponding to
	// its interval value.
	// Schedule is never called concurrently, the next call to schedule takes
	// place `intv` time after the previous call completed.
	// RequestProduction() is the means of scheduling for the
	// SchedulingController.
	// During Schedule, the SchedulingController should request the enqueueing
	// of new tasks by calling RequestProduction() on the QueueScheduler.
	Schedule()
}

type ProductionRequest struct {
	N uint
}

type QueueSchedulerConfig struct {
	Namespace string
	JobName   string

	Pool   *redis.Pool        `yaml:"-"`
	Logger logrus.FieldLogger `yaml:"-"`

	Controller SchedulingController
}

var QueueSchedulerDefaultConfig = &QueueSchedulerConfig{}

type QueueScheduler struct {
	// Config contains the configuration of the QueueScheduler.
	// Config must not be modified after Start() has been called.
	Config *QueueSchedulerConfig

	tomb *tomb.Tomb

	enqueuer    *work.Enqueuer
	client      *work.Client
	fieldLogger logrus.FieldLogger

	c chan ProductionRequest

	enqueuedJobs uint32

	interval time.Duration
}

func NewQueueScheduler(config *QueueSchedulerConfig) *QueueScheduler {
	return &QueueScheduler{
		Config: config,
		c:      make(chan ProductionRequest),
	}
}

func (q *QueueScheduler) Start(ctx context.Context) {
	q.fieldLogger = q.Config.Logger.WithFields(logrus.Fields{
		"namespace": q.Config.Namespace,
		"jobname":   q.Config.JobName,
		"package":   "workqueue",
		"component": "QueueScheduler",
	})

	q.tomb, _ = tomb.WithContext(ctx)
	q.enqueuer = work.NewEnqueuer(q.Config.Namespace, q.Config.Pool)
	q.client = work.NewClient(q.Config.Namespace, q.Config.Pool)

	q.fieldLogger.Info("Performing scheduling controller initialisation")
	q.Config.Controller.Init(q)

	q.tomb.Go(q.run)
}

func (q *QueueScheduler) SignalStop() {
	q.tomb.Kill(lifecycle.ErrStopSignalled)
}

func (q *QueueScheduler) Wait() error {
	return q.tomb.Wait()
}

func (q *QueueScheduler) Dead() <-chan struct{} {
	return q.tomb.Dead()
}

func (q *QueueScheduler) Err() error {
	return q.tomb.Err()
}

func (q *QueueScheduler) run() error {
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

//
// This method is meant to be part of the lower facing API (towards
// SchedulingController).
func (q *QueueScheduler) GetEnqueued() uint {
	return uint(q.enqueuedJobs)
}

//
// This method is meant to be part of the lower facing API (towards
// SchedulingController).
func (q *QueueScheduler) GetEnqueuedAndReset() uint {
	return uint(atomic.SwapUint32(&q.enqueuedJobs, 0))
}

//
// This method is meant to be part of the lower facing API (towards
// SchedulingController).
func (q *QueueScheduler) SetInterval(interval time.Duration) {
	q.fieldLogger.WithField("interval", interval).Debug("Setting interval")
	q.interval = interval
}

//
// This method is meant to be part of the lower facing API (towards
// SchedulingController).
func (q *QueueScheduler) GetQueueInfo() (count int64, latency int64, err error) {
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
// with the queuing system.
// This is equal to the total currency of the queueing system.
//
// This method is meant to be part of the lower facing API (towards
// SchedulingController).
func (q *QueueScheduler) GetWorkerNum() (int, error) {
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
// This method is meant to be part of the lower facing API (towards
// SchedulingController).
func (q *QueueScheduler) GetWorkerPoolNum() (int, error) {
	heartbeats, err := q.client.WorkerPoolHeartbeats()
	if err != nil {
		return 0, err
	}

	return len(heartbeats), nil
}

//
// This method is meant to be part of the lower facing API (towards
// SchedulingController).
func (q *QueueScheduler) RequestProduction(n uint) {
	q.fieldLogger.WithField("n", n).Debug("Requesting production")
	request := ProductionRequest{
		N: n,
	}

	select {
	case q.c <- request:
	case <-q.tomb.Dying():
	}
}

//
// This method is meant to be part of the upper facing API (towards Producer).
func (q *QueueScheduler) Enqueue(args map[string]interface{}) (*work.Job, error) {
	job, err := q.enqueuer.Enqueue(q.Config.JobName, args)
	if err != nil {
		return job, err
	}

	atomic.AddUint32(&q.enqueuedJobs, 1)

	return job, err
}

// C returns a channel producers should listen on. When a ProductionRequest
// with N = 6 is received, 6 items should be enqueued by the Producer (using
// the Enqueue() method).
//
// C returns the same channel for all calls. That means ProdutionRequest
// are randomly assigned to a producer.
//
// Producers should take care to process ProductionRequests quickly (or
// out-of-line). QueueScheduler blocks until the ProductionRequest is consumed
// from the channel.
//
// This method is meant to be part of the upper facing API (towards Producer).
func (q *QueueScheduler) C() <-chan ProductionRequest {
	return q.c
}

type EWMASchedulerConfig struct {
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

var EWMASchedulerDefaultConfig = &EWMASchedulerConfig{
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

type ewmaSchedulerPhase int

// Constants related to EWMAScheduler
const (
	esp_Uninitialised ewmaSchedulerPhase = iota
	esp_StartUp
	esp_Maintaining
)

var _ SchedulingController = &EWMAScheduler{}

type EWMAScheduler struct {
	Config *EWMASchedulerConfig

	queueScheduler *QueueScheduler
	fieldLogger    logrus.FieldLogger

	phase               ewmaSchedulerPhase
	startUpStepCount    uint
	previousScheduling  time.Time
	previousQueueLength uint
	threshold           uint
	// deviationEWMA is normalised to deviation (of consumption) per second
	deviationEWMA float64
	// consumptionEWMA is normalised to consumption per second
	consumptionEWMA float64
}

func NewEWMAScheduler(config *EWMASchedulerConfig) *EWMAScheduler {
	return &EWMAScheduler{
		Config: config,
	}
}

func (e *EWMAScheduler) Init(queueScheduler *QueueScheduler) {
	e.queueScheduler = queueScheduler
	e.fieldLogger = queueScheduler.Config.Logger.WithFields(logrus.Fields{
		"package":   "workqueue",
		"component": "EWMAScheduler",
	})

	// Initialise state
	e.phase = esp_StartUp
	e.startUpStepCount = 0
	e.previousQueueLength = 0
	e.deviationEWMA = 0
	e.consumptionEWMA = 0

	// Initial scheduling
	e.scheduleInit()
}

func (e *EWMAScheduler) scheduleInit() {
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

	e.fieldLogger.WithFields(logrus.Fields{
		"queue_length": queueLength,
		"threshold":    e.threshold,
		"worker_num":   workerNum,
	}).Debug("Planning production request")
	if queueLength < int64(e.threshold) {
		e.queueScheduler.RequestProduction(e.threshold - uint(queueLength))
	}
}

func (e *EWMAScheduler) Schedule() {
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
	enqueued := e.queueScheduler.GetEnqueuedAndReset()

	workerNum, err := e.queueScheduler.GetWorkerNum()
	if err != nil {
		// No scheduling possible
		return
	}

	var consumption uint
	if e.previousQueueLength+enqueued < uint(queueLength) {
		consumption = 0
	} else {
		consumption = e.previousQueueLength + enqueued - uint(queueLength)
	}

	deviation := float64(consumption) - normaliseDuration(timePassed, time.Second, e.consumptionEWMA)
	if deviation < 0 {
		deviation = -deviation
	}

	consumptionAlpha := 1 - math.Exp(-normaliseDuration(timePassed, e.Config.ConsumptionLifetime))
	e.consumptionEWMA = updateEwma(e.consumptionEWMA, consumptionAlpha, normaliseDuration(time.Second, timePassed, float64(consumption)))

	e.deviationEWMA = updateEwma(e.deviationEWMA, e.Config.MaintainingDeviationAlpha, normaliseDuration(time.Second, timePassed, deviation))

	e.fieldLogger.WithFields(logrus.Fields{
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
		e.fieldLogger.WithField("previous_threshold", prevThreshold).WithField("threshold", e.threshold).Debug("Exhausted, doubled threshold")
	} else {
		switch e.phase {
		case esp_StartUp:
			e.startUpStepCount += 1
			e.threshold = e.minLength(e.startupThreshold(uint(workerNum)), uint(workerNum))

			e.fieldLogger.WithField("threshold", e.threshold).WithField("phase", "startup").Debug("Calculated threshold")
			if e.startUpStepCount >= e.Config.StartUpSteps {
				e.phase = esp_Maintaining
				e.queueScheduler.SetInterval(e.Config.MaintainingInterval)

				e.threshold = e.minLength(e.maintainingThreshold(), uint(workerNum))
				e.fieldLogger.WithField("threshold", e.threshold).WithField("phase", "startup").Debug("Switching to maintaining phase")
			}
		case esp_Maintaining:
			e.threshold = e.minLength(e.maintainingThreshold(), uint(workerNum))
			e.fieldLogger.WithField("threshold", e.threshold).WithField("phase", "maintaining").Debug("Calculated threshold")
		default:
			e.threshold = e.minLength(0, uint(workerNum))
			e.fieldLogger.WithField("threshold", e.threshold).WithField("phase", "unknown").Debug("Calculated threshold")
		}
	}

	e.fieldLogger.WithFields(logrus.Fields{
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
func (e *EWMAScheduler) startupThreshold(workerNum uint) uint {
	return uint(workerNum) * e.Config.StartUpWorkerTheshold
}

// maintainingThreshold calculates the threshold during the maintaining phase
// from the ewma fields which are tracked as part of the state.
func (e *EWMAScheduler) maintainingThreshold() uint {
	expDeviation := normaliseDuration(e.Config.MaintainingInterval, time.Second, e.deviationEWMA)
	expConsumption := normaliseDuration(e.Config.MaintainingInterval, time.Second, e.consumptionEWMA)

	calculatedThreshold := math.Ceil(e.Config.MaintainingDeviationFactor*expDeviation + expConsumption)

	e.fieldLogger.WithFields(logrus.Fields{
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
func (e *EWMAScheduler) minLength(calculatedLength uint, workerNum uint) uint {
	// Calculate minLength based on the MinWorkerThreshold config option and the workerNum
	minLength := uint(math.Ceil(e.Config.MinWorkerThreshold * float64(workerNum)))

	if e.Config.MinThreshold > minLength {
		minLength = e.Config.MinThreshold
	}

	if calculatedLength > minLength {
		minLength = calculatedLength
	}

	return minLength
}

//
// (1 - alpha) * ewma + alpha * value
func updateEwma(ewma, alpha, value float64) float64 {
	return (1-alpha)*ewma + alpha*value
}

//
// (duration / normalDuration) * factor_1 * factor_2 * ...
func normaliseDuration(duration, normalDuration time.Duration, factors ...float64) float64 {
	res := float64(duration.Nanoseconds()) / float64(normalDuration.Nanoseconds())

	for _, factor := range factors {
		res *= factor
	}

	return res
}
