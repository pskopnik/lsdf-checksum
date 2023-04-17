package scheduler

import (
	"math"
	"time"

	"github.com/apex/log"
)

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

	Logger log.Interface `yaml:"-"`
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

	scheduler   ControllerScheduler
	fieldLogger log.Interface

	phase            ewmaControllerPhase
	startUpStepCount uint

	previousScheduling           time.Time
	previousQueueLength          uint
	previousEnqueuedCounter      uint64
	previousObservedEmptyCounter uint64

	// consumptionEWMA is the normalised consumption (jobs dequeued from the
	// queue) per second.
	consumptionEWMA float64
	// deviationEWMA is the normalised deviation (of consumption) per second.
	deviationEWMA float64

	// threshold is the targeted length of the queue. This is the main output
	// variable of the controller.
	threshold uint
}

func NewEWMAController(config *EWMAControllerConfig) *EWMAController {
	return &EWMAController{
		Config: config,
	}
}

func (e *EWMAController) Init(queueScheduler ControllerScheduler) {
	e.scheduler = queueScheduler
	e.fieldLogger = e.Config.Logger.WithFields(log.Fields{
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
	e.scheduler.SetInterval(e.Config.StartUpInterval)

	var workerCount, queueLength uint64
	queue := e.scheduler.GetQueue()
	queueInfo, err := queue.GetQueueInfo()
	if err == nil {
		queueLength = queueInfo.QueuedJobs
	}
	workerInfo, err := queue.GetWorkerInfo()
	if err == nil {
		workerCount = workerInfo.WorkerNum
	}

	e.threshold = e.minLength(uint(workerCount)*e.Config.StartUpWorkerTheshold, uint(workerCount))

	e.fieldLogger.WithFields(log.Fields{
		"threshold": e.threshold,
		"phase":     "startup",
	}).Debug("Calculated initial threshold")

	e.fieldLogger.WithFields(log.Fields{
		"queue_length": queueLength,
		"threshold":    e.threshold,
		"worker_count":   workerCount,
	}).Debug("Planning production request")
	if queueLength < uint64(e.threshold) {
		e.scheduler.RequestProductionUntilThreshold(e.threshold - uint(queueLength))
	}
}

func (e *EWMAController) Schedule() {
	now := time.Now()
	timePassed := now.Sub(e.previousScheduling)
	e.previousScheduling = now

	schedulerStats := e.scheduler.Stats()

	queue := e.scheduler.GetQueue()
	queueInfo, err := queue.GetQueueInfo()
	if err != nil {
		e.fieldLogger.WithError(err).WithField("action", "skipping").
			Debug("No scheduling possible, couldn't get queue info")
		return
	}
	workerInfo, err := queue.GetWorkerInfo()
	if err != nil {
		e.fieldLogger.WithError(err).WithField("action", "skipping").
			Debug("No scheduling possible, couldn't get worker info")
		return
	}
	queueLength := queueInfo.QueuedJobs
	workerCount := workerInfo.WorkerNum

	enqueuedCounter := schedulerStats.JobsEnqueued
	enqueued := enqueuedCounter - e.previousEnqueuedCounter
	e.previousEnqueuedCounter = enqueuedCounter

	observedEmptyCounter := schedulerStats.QueueObservedEmpty
	exhausted := observedEmptyCounter > e.previousObservedEmptyCounter
	e.previousObservedEmptyCounter = observedEmptyCounter

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
		"outstanding_orders":    schedulerStats.OrdersInQueue + schedulerStats.OrdersInProgress,
		"worker_count":            workerCount,
		"consumption":           consumption,
		"deviation":             deviation,
		"consumption_alpha":     consumptionAlpha,
		"consumption_ewma":      e.consumptionEWMA,
		"deviation_alpha":       e.Config.MaintainingDeviationAlpha,
		"deviation_ewma":        e.deviationEWMA,
	}).Debug("Probed queue and calculated derivatives")

	e.previousQueueLength = uint(queueLength)

	if exhausted {
		// FUTR: Consider returning to startup phase, with more frequent probing?

		prevThreshold := e.threshold
		e.threshold = e.minLength(2*consumption, uint(workerCount))

		e.fieldLogger.WithFields(log.Fields{
			"previous_threshold": prevThreshold,
			"threshold":          e.threshold,
			"consumption":        consumption,
			"enqueued":           enqueued,
			"outstanding_orders": schedulerStats.OrdersInQueue + schedulerStats.OrdersInProgress,
			"queue_length":       queueLength,
		}).Debug("Exhausted, setting threshold to at least double the consumption")
	} else {
		switch e.phase {
		case espStartUp:
			e.startUpStepCount++
			if e.startUpStepCount < e.Config.StartUpSteps {
				e.threshold = e.minLength(e.startupThreshold(uint(workerCount)), uint(workerCount))
				e.fieldLogger.WithFields(log.Fields{
					"threshold": e.threshold,
					"phase":     "startup",
				}).Debug("Calculated threshold")
			} else {
				e.phase = espMaintaining
				e.scheduler.SetInterval(e.Config.MaintainingInterval)

				e.threshold = e.minLength(e.maintainingThreshold(), uint(workerCount))
				e.fieldLogger.WithFields(log.Fields{
					"threshold": e.threshold,
					"phase":     "startup",
				}).Debug("Switched to maintaining phase and calculated threshold")
			}
		case espMaintaining:
			e.threshold = e.minLength(e.maintainingThreshold(), uint(workerCount))
			e.fieldLogger.WithFields(log.Fields{
				"threshold": e.threshold,
				"phase":     "maintaining",
			}).Debug("Calculated threshold")
		default:
			e.threshold = e.minLength(0, uint(workerCount))
			e.fieldLogger.WithFields(log.Fields{
				"threshold": e.threshold,
				"phase":     "unknown",
			}).Debug("Calculated threshold")
		}
	}

	e.fieldLogger.WithFields(log.Fields{
		"queue_length": queueLength,
		"threshold":    e.threshold,
		"worker_count":   workerCount,
	}).Debug("Planning production request")
	if queueLength < uint64(e.threshold) {
		e.scheduler.RequestProductionUntilThreshold(e.threshold - uint(queueLength))
	}
}

// startupThreshold calculates the threshold during the startup phase using
// only the current number of workers and the StartUpWorkerTheshold config
// option.
func (e *EWMAController) startupThreshold(workerCount uint) uint {
	return uint(workerCount) * e.Config.StartUpWorkerTheshold
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
func (e *EWMAController) minLength(calculatedLength uint, workerCount uint) uint {
	// Calculate minLength based on the MinWorkerThreshold config option and the workerCount
	minLength := uint(math.Ceil(e.Config.MinWorkerThreshold * float64(workerCount)))

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
