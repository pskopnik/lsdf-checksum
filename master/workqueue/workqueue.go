// Package workqueue
package workqueue

import (
	"context"

	"github.com/apex/log"
	"github.com/gomodule/redigo/redis"
	"gopkg.in/tomb.v2"

	"git.scc.kit.edu/sdm/lsdf-checksum/internal/lifecycle"
	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
	"git.scc.kit.edu/sdm/lsdf-checksum/workqueue"
	"git.scc.kit.edu/sdm/lsdf-checksum/workqueue/scheduler"
)

//go:generate confions config Config

type Config struct {
	FileSystemName string
	RedisPrefix    string

	RunID        uint64
	SnapshotName string

	DB     *meda.DB      `yaml:"-"`
	Logger log.Interface `yaml:"-"`
	Pool   *redis.Pool   `yaml:"-"`

	// EWMAController contains the configuration for the EWMAController.
	// Here only static configuration options should be set.
	// All known run time dependent options (database connections, RunID, etc.)
	// will be overwritten when the final configuration is assembled.
	EWMAController scheduler.EWMAControllerConfig
	// Producer contains the configuration for the Producer. Here only static
	// configuration options should be set.
	// All known run time dependent options (database connections, RunID, etc.)
	// will be overwritten when the final configuration is assembled.
	Producer ProducerConfig
	// QueueWatcher contains the configuration for the QueueWatcher. Here only
	// static configuration options should be set.
	// All known run time dependent options (database connections, RunID, etc.)
	// will be overwritten when the final configuration is assembled.
	QueueWatcher QueueWatcherConfig
	// WriteBacker contains the configuration for the WriteBacker. Here only
	// static configuration options should be set.
	// All known run time dependent options (database connections, RunID, etc.)
	// will be overwritten when the final configuration is assembled.
	WriteBacker WriteBackerConfig
	// PerformanceMonitor contains the configuration for the PerformanceMonitor.
	// Here only static configuration options should be set.
	// All known run time dependent options (database connections, RunID, etc.)
	// will be overwritten when the final configuration is assembled.
	PerformanceMonitor PerformanceMonitorConfig
}

var DefaultConfig = &Config{}

type WorkQueue struct {
	Config *Config

	tomb *tomb.Tomb

	fieldLogger log.Interface

	workqueue            *workqueue.Workqueue
	publisher            *workqueue.DConfigPublisher
	schedulingController scheduler.Controller
	producer             *Producer
	writeBacker          *WriteBacker
	queueWatcher         *QueueWatcher
	performanceMonitor   *PerformanceMonitor
}

func New(config *Config) *WorkQueue {
	return &WorkQueue{
		Config: config,
	}
}

func (w *WorkQueue) Start(ctx context.Context) {
	w.fieldLogger = w.Config.Logger.WithFields(log.Fields{
		"run":        w.Config.RunID,
		"snapshot":   w.Config.SnapshotName,
		"filesystem": w.Config.FileSystemName,
		"component":  "workqueue.WorkQueue",
	})

	w.tomb, _ = tomb.WithContext(ctx)

	w.tomb.Go(func() error {
		w.workqueue = w.createWorkqueue()
		w.publisher = w.workqueue.DConfig().StartPublisher(w.tomb.Context(nil))

		w.schedulingController = w.createEWMAController()

		w.producer = w.createProducer(w.schedulingController)
		w.producer.Start(w.tomb.Context(nil))

		w.queueWatcher = w.createQueueWatcher(w.producer.Dead())
		w.queueWatcher.Start(w.tomb.Context(nil))

		w.writeBacker = w.createWriteBacker()
		w.writeBacker.Start(w.tomb.Context(nil))

		w.performanceMonitor = w.createPerformanceMonitor(w.workqueue, w.publisher)
		w.performanceMonitor.Start(w.tomb.Context(nil))

		w.tomb.Go(w.writeBackerStopper)
		w.tomb.Go(w.performanceMonitorStopper)

		w.tomb.Go(w.producerManager)
		w.tomb.Go(w.queueWatcherManager)
		w.tomb.Go(w.writeBackerManager)
		w.tomb.Go(w.performanceMonitorManager)

		w.tomb.Go(w.waiter)

		return nil
	})
}

func (w *WorkQueue) SignalStop() {
	w.tomb.Kill(lifecycle.ErrStopSignalled)
}

func (w *WorkQueue) Wait() error {
	return w.tomb.Wait()
}

func (w *WorkQueue) Dead() <-chan struct{} {
	return w.tomb.Dead()
}

func (w *WorkQueue) Err() error {
	return w.tomb.Err()
}

func (w *WorkQueue) producerManager() error {
	select {
	case <-w.producer.Dead():
		return w.producer.Err()
	case <-w.tomb.Dying():
		return tomb.ErrDying
	}
}

func (w *WorkQueue) queueWatcherManager() error {
	select {
	case <-w.queueWatcher.Dead():
		return w.queueWatcher.Err()
	case <-w.tomb.Dying():
		return tomb.ErrDying
	}
}

func (w *WorkQueue) writeBackerManager() error {
	select {
	case <-w.writeBacker.Dead():
		return w.writeBacker.Err()
	case <-w.tomb.Dying():
		return tomb.ErrDying
	}
}

func (w *WorkQueue) performanceMonitorManager() error {
	select {
	case <-w.performanceMonitor.Dead():
		err := w.performanceMonitor.Err()
		if err == lifecycle.ErrStopSignalled {
			return nil
		} else {
			return err
		}
	case <-w.tomb.Dying():
		return tomb.ErrDying
	}
}

func (w *WorkQueue) waiter() error {
	w.producer.Wait()
	w.queueWatcher.Wait()
	w.writeBacker.Wait()
	w.performanceMonitor.Wait()

	return nil
}

func (w *WorkQueue) writeBackerStopper() error {
	select {
	case <-w.queueWatcher.Dead():
		w.writeBacker.SignalEndOfQueue()
		return nil
	case <-w.tomb.Dying():
		return tomb.ErrDying
	}
}

func (w *WorkQueue) performanceMonitorStopper() error {
	select {
	case <-w.queueWatcher.Dead():
		w.performanceMonitor.SignalStop()
		return nil
	case <-w.tomb.Dying():
		return tomb.ErrDying
	}
}

func (w *WorkQueue) createWorkqueue() *workqueue.Workqueue {
	return workqueue.New(
		w.Config.Pool,
		w.Config.RedisPrefix,
		w.Config.FileSystemName,
		w.Config.SnapshotName,
	)
}

func (w *WorkQueue) createEWMAController() *scheduler.EWMAController {
	config := scheduler.EWMAControllerDefaultConfig.
		Clone().
		Merge(&w.Config.EWMAController).
		Merge(&scheduler.EWMAControllerConfig{})

	return scheduler.NewEWMAController(config)
}

func (w *WorkQueue) createProducer(controller scheduler.Controller) *Producer {
	config := ProducerDefaultConfig.
		Clone().
		Merge(&w.Config.Producer).
		Merge(&ProducerConfig{
			FileSystemName: w.Config.FileSystemName,
			Namespace:      workqueue.GocraftWorkNamespace(w.Config.RedisPrefix),

			SnapshotName: w.Config.SnapshotName,

			Pool:   w.Config.Pool,
			DB:     w.Config.DB,
			Logger: w.Config.Logger,

			Controller: controller,
		})

	return NewProducer(config)
}

func (w *WorkQueue) createWriteBacker() *WriteBacker {
	config := WriteBackerDefaultConfig.
		Clone().
		Merge(&w.Config.WriteBacker).
		Merge(&WriteBackerConfig{
			FileSystemName: w.Config.FileSystemName,
			Namespace:      workqueue.GocraftWorkNamespace(w.Config.RedisPrefix),

			RunID:        w.Config.RunID,
			SnapshotName: w.Config.SnapshotName,

			Pool:   w.Config.Pool,
			DB:     w.Config.DB,
			Logger: w.Config.Logger,
		})

	return NewWriteBacker(config)
}

func (w *WorkQueue) createQueueWatcher(productionExhausted <-chan struct{}) *QueueWatcher {
	config := QueueWatcherDefaultConfig.
		Clone().
		Merge(&w.Config.QueueWatcher).
		Merge(&QueueWatcherConfig{
			FileSystemName: w.Config.FileSystemName,
			Namespace:      workqueue.GocraftWorkNamespace(w.Config.RedisPrefix),

			RunID:        w.Config.RunID,
			SnapshotName: w.Config.SnapshotName,

			Pool:   w.Config.Pool,
			Logger: w.Config.Logger,

			ProductionExhausted: productionExhausted,
		})

	return NewQueueWatcher(config)
}

func (w *WorkQueue) createPerformanceMonitor(
	wq *workqueue.Workqueue,
	publisher *workqueue.DConfigPublisher,
) *PerformanceMonitor {
	config := PerformanceMonitorDefaultConfig.
		Clone().
		Merge(&w.Config.PerformanceMonitor).
		Merge(&PerformanceMonitorConfig{
			Workqueue: wq,
			Publisher: publisher,
			Logger:    w.Config.Logger,
		})

	return NewPerformanceMonitor(config)
}
