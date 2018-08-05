package workqueue

import (
	"context"
	"errors"
	"time"

	"github.com/gocraft/work"
	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
	"gopkg.in/tomb.v2"
)

var (
	ErrMaxChecksExceeded = errors.New("MaxRetryJobChecks exceeded")
)

//go:generate confions config QueueWatcherConfig

type QueueWatcherConfig struct {
	CheckInterval     time.Duration
	MaxRetryJobChecks uint

	FileSystemName string
	Namespace      string

	RunId        uint64
	SnapshotName string

	Pool   *redis.Pool
	Logger logrus.FieldLogger

	// ProductionExhausted is a channel which must be closed as soon as all
	// items have been enqueued, i.e. production is exhausted.
	ProductionExhausted <-chan struct{}
}

var QueueWatcherDefaultConfig = QueueWatcherConfig{
	CheckInterval:     time.Second * 10,
	MaxRetryJobChecks: 50,
}

type QueueWatcher struct {
	Config *QueueWatcherConfig

	tomb *tomb.Tomb

	client *work.Client

	fieldLogger logrus.FieldLogger
}

func NewQueueWatcher(config *QueueWatcherConfig) *QueueWatcher {
	return &QueueWatcher{
		Config: config,
	}
}

func (q *QueueWatcher) Start(ctx context.Context) {
	q.tomb, _ = tomb.WithContext(ctx)

	q.tomb.Go(q.run)
}

func (q *QueueWatcher) SignalStop() {
	q.tomb.Kill(stopSignalled)
}

func (q *QueueWatcher) Wait() error {
	return q.tomb.Wait()
}

func (q *QueueWatcher) Dead() <-chan struct{} {
	return q.tomb.Dead()
}

func (q *QueueWatcher) Err() error {
	return q.tomb.Err()
}

func (q *QueueWatcher) run() error {
	var err error

	q.fieldLogger = q.Config.Logger.WithFields(logrus.Fields{
		"run":        q.Config.RunId,
		"snapshot":   q.Config.SnapshotName,
		"filesystem": q.Config.FileSystemName,
		"namespace":  q.Config.Namespace,
		"package":    "workqueue",
		"component":  "QueueWatcher",
	})

	q.fieldLogger.Info("Starting watching")

	q.fieldLogger.Info("Waiting for production to finish")

	select {
	case <-q.Config.ProductionExhausted:
		break
	case <-q.tomb.Dying():
		q.fieldLogger.Info("Stopping watching as component is dying")
		return tomb.ErrDying
	}

	q.client = work.NewClient(q.Config.Namespace, q.Config.Pool)

	ctx := q.tomb.Context(nil)

	q.fieldLogger.Info("Waiting for CalculateChecksum jobs to finish processing")

	err = q.waitForJobsFinished(ctx, CalculateChecksumJobName)
	if err != nil {
		if err == context.Canceled {
			// Tomb has canceled the context passed to waitForJobsFinished
			q.fieldLogger.Info("Stopping watching as component is dying")
			return tomb.ErrDying
		}
		q.fieldLogger.WithError(err).WithField("action", "stopping").
			Error("Encountered error while waiting for CalculateChecksum jobs to finish processing")
		return err
	}

	q.fieldLogger.Info("Waiting for WriteBack jobs to finish processing")

	err = q.waitForJobsFinished(ctx, WriteBackJobName(q.Config.FileSystemName, q.Config.SnapshotName))
	if err != nil {
		if err == context.Canceled {
			// Tomb has canceled the context passed to waitForJobsFinished
			q.fieldLogger.Info("Stopping watching as component is dying")
			return tomb.ErrDying
		}
		q.fieldLogger.WithError(err).WithField("action", "stopping").
			Error("Encountered error while waiting for WriteBack jobs to finish processing")
		return err
	}

	q.fieldLogger.WithField("action", "stopping").Info("Finished watching")

	return nil
}

func (q *QueueWatcher) watch(ctx context.Context, jobName string) <-chan struct{} {
	done := make(chan struct{})

	go func() {
		// TODO error
		_ = q.waitForJobsFinished(ctx, jobName)
		close(done)
	}()

	return done
}

func (q *QueueWatcher) waitForJobsFinished(ctx context.Context, jobName string) error {
	var empty bool
	var err error

	timer := time.NewTimer(time.Duration(0))
	done := ctx.Done()
	first := true

	// Exhaust timer
	if !timer.Stop() {
		<-timer.C
	}

L:
	for {
		if first {
			timer.Reset(time.Duration(0))
			first = false
		} else {
			timer.Reset(q.Config.CheckInterval)
		}

		select {
		case <-done:
			// Exhaust timer
			if !timer.Stop() {
				<-timer.C
			}

			return ctx.Err()
		case <-timer.C:
		}

		empty, err = q.checkQueueEmpty(jobName)
		if err != nil {
			return err
		}
		q.fieldLogger.WithFields(logrus.Fields{
			"method":  "waitForJobsFinished",
			"jobname": jobName,
			"empty":   empty,
		}).Debug("Checked queue empty")
		if !empty {
			continue L
		}

		// Check the following twice, as jobs might move between the two states
		// "processing" and "queued in retry queue"
		for i := 0; i < 2; i++ {
			empty, err = q.checkWorkerObservationsEmpty(jobName)
			if err != nil {
				return err
			}
			q.fieldLogger.WithFields(logrus.Fields{
				"method":  "waitForJobsFinished",
				"jobname": jobName,
				"empty":   empty,
			}).Debug("Checked worker observations empty")
			if !empty {
				continue L
			}

			empty, err = q.checkRetryJobsEmpty(jobName)
			if err != nil {
				return err
			}
			q.fieldLogger.WithFields(logrus.Fields{
				"method":  "waitForJobsFinished",
				"jobname": jobName,
				"empty":   empty,
			}).Debug("Checked retry jobs empty")
			if !empty {
				continue L
			}
		}

		break L
	}

	return nil
}

// checkQueueEmpty checks if the queue with the passed in name (JobName in
// gocraft/work) is empty. Returns true if the queue is empty, false if not.
func (q *QueueWatcher) checkQueueEmpty(jobName string) (bool, error) {
	queues, err := q.client.Queues()
	if err != nil {
		return false, err
	}

	for _, queue := range queues {
		if queue.JobName == jobName {
			return queue.Count == 0, nil
		}
	}

	// If queue does not exist consider it empty.
	// QueueWatcher is not active in the initial phase during which no items
	// have been enqueued yet.
	// E.g. when active checks start, the ProductionExhausted channel has been
	// closed.
	return true, nil
}

func (q *QueueWatcher) checkWorkerObservationsEmpty(jobName string) (bool, error) {
	observations, err := q.client.WorkerObservations()
	if err != nil {
		return false, err
	}

	for _, observation := range observations {
		if observation.IsBusy && observation.JobName == jobName {
			return false, nil
		}
	}

	return true, nil
}

func (q *QueueWatcher) checkRetryJobsEmpty(jobName string) (bool, error) {
	var err error
	var firstJob *work.RetryJob
	var page uint
	var i, jobsReturned, totalJobs int64
	var retryJobs []*work.RetryJob

	for checksCounter := uint(0); checksCounter < q.Config.MaxRetryJobChecks; checksCounter++ {
		totalJobs = 1

		// Iterate over all pages, each page containing 20 items. Page indexing
		// starts at 1.
		// https://godoc.org/github.com/gocraft/work#Client.RetryJobs
		for i, page = 0, 1; i < totalJobs; i, page = i+jobsReturned, page+1 {
			retryJobs, totalJobs, err = q.client.RetryJobs(page)
			if err != nil {
				// TODO error
				return false, err
			}
			jobsReturned = int64(len(retryJobs))

			if i == 0 && len(retryJobs) > 0 {
				firstJob = retryJobs[0]
			}

			for _, retryJob := range retryJobs {
				if retryJob.Name == jobName {
					return false, nil
				}
			}
		}

		if page > 2 {
			// Multiple pages were fetched

			// Check first job to see if jobs have moved up in the retry queue
			// and thereby have broken pagination
			retryJobs, _, err := q.client.RetryJobs(1)
			if err != nil {
				// TODO error
				return false, err
			}

			if len(retryJobs) == 0 || retryJobs[0].ID != firstJob.ID {
				return false, nil
			}
		}

		return true, nil
	}

	return false, ErrMaxChecksExceeded
}
