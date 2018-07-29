// Package workqueue
package workqueue

import (
	"context"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"gopkg.in/tomb.v2"
)

//go:generate confions config Config

type Config struct {
	FileSystemName string

	RunId        int
	SnapshotName string

	DB     *sqlx.DB
	Logger logrus.FieldLogger

	Redis RedisConfig
	// EWMAScheduler contains the configuration for the EWMAScheduler
	// SchedulingController. Here only static configuration options
	// should be set. All known run time dependent options (database
	// connections, RunId, etc.) will be overwritten when the final
	// configuration is assembled.
	EWMAScheduler EWMASchedulerConfig
	// Producer contains the configuration for the Producer. Here only
	// static configuration options should be set.
	// All known run time dependent options (database connections, RunId, etc.)
	// will be overwritten when the final configuration is assembled.
	Producer ProducerConfig
	// QueueWatcher contains the configuration for the QueueWatcher. Here only
	// static configuration options should be set.
	// All known run time dependent options (database connections, RunId, etc.)
	// will be overwritten when the final configuration is assembled.
	QueueWatcher QueueWatcherConfig
	// WriteBacker contains the configuration for the WriteBacker. Here only
	// static configuration options should be set.
	// All known run time dependent options (database connections, RunId, etc.)
	// will be overwritten when the final configuration is assembled.
	WriteBacker WriteBackerConfig
}

// RedisConfig contains configuration options for a connection pool to a redis
// database.
type RedisConfig struct {
	Network  string
	Address  string
	Database int
	// Namespace contains the namespace used in gocraft/work.
	Namespace   string
	Password    string
	MaxIdle     int
	IdleTimeout time.Duration
}

var DefaultConfig = &Config{
	Redis: RedisConfig{
		MaxIdle:     10,
		IdleTimeout: 300 * time.Second,
		Namespace:   "lsdf-checksum/workqueue",
	},
}

type WorkQueue struct {
	Config *Config

	tomb *tomb.Tomb

	fieldLogger logrus.FieldLogger
	pool        *redis.Pool

	schedulingController SchedulingController
	producer             *Producer
	writeBacker          *WriteBacker
	queueWatcher         *QueueWatcher
}

func New(config *Config) *WorkQueue {
	return &WorkQueue{
		Config: config,
	}
}

func (w *WorkQueue) Start(ctx context.Context) {
	w.fieldLogger = w.Config.Logger.WithFields(logrus.Fields{
		"run":        w.Config.RunId,
		"snapshot":   w.Config.SnapshotName,
		"filesystem": w.Config.FileSystemName,
		"package":    "workqueue",
		"component":  "WorkQueue",
	})

	w.pool = &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.Dial(
				w.Config.Redis.Network,
				w.Config.Redis.Address,
				redis.DialDatabase(w.Config.Redis.Database),
				redis.DialPassword(w.Config.Redis.Password),
			)
		},
		MaxIdle:     w.Config.Redis.MaxIdle,
		IdleTimeout: w.Config.Redis.IdleTimeout,
	}

	w.tomb, _ = tomb.WithContext(ctx)

	w.tomb.Go(func() error {
		err := w.testRedis()
		if err != nil {
			return err
		}

		w.schedulingController = w.createEWMAScheduler()

		w.producer = w.createProducer(w.schedulingController)
		w.producer.Start(w.tomb.Context(nil))

		w.queueWatcher = w.createQueueWatcher(w.producer.Dead())
		w.queueWatcher.Start(w.tomb.Context(nil))

		w.writeBacker = w.createWriteBacker()
		w.writeBacker.Start(w.tomb.Context(nil))

		w.tomb.Go(w.writeBackerStopper)

		w.tomb.Go(w.producerManager)
		w.tomb.Go(w.queueWatcherManager)
		w.tomb.Go(w.writeBackerManager)
		w.tomb.Go(w.waiter)

		return nil
	})
}

func (w *WorkQueue) SignalStop() {
	w.tomb.Kill(stopSignalled)
}

func (w *WorkQueue) Wait() {
	<-w.tomb.Dead()
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
	}

	return nil
}

func (w *WorkQueue) queueWatcherManager() error {
	select {
	case <-w.queueWatcher.Dead():
		return w.queueWatcher.Err()
	case <-w.tomb.Dying():
	}

	return nil
}

func (w *WorkQueue) writeBackerManager() error {
	select {
	case <-w.writeBacker.Dead():
		return w.writeBacker.Err()
	case <-w.tomb.Dying():
	}

	return nil
}

func (w *WorkQueue) waiter() error {
	w.producer.Wait()
	w.queueWatcher.Wait()
	w.writeBacker.Wait()

	return nil
}

func (w *WorkQueue) writeBackerStopper() error {
	select {
	case <-w.queueWatcher.Dead():
		break
	case <-w.tomb.Dying():
		break
	}

	w.writeBacker.SignalEndOfQueue()

	return nil
}

func (w *WorkQueue) testRedis() error {
	loggerFields := logrus.Fields{
		"network":       w.Config.Redis.Network,
		"address":       w.Config.Redis.Address,
		"database":      w.Config.Redis.Database,
		"authenticated": len(w.Config.Redis.Password) > 0,
	}

	conn := w.pool.Get()
	_, err := conn.Do("PING")
	_ = conn.Close()
	if err != nil {
		w.fieldLogger.
			WithFields(loggerFields).
			WithError(err).
			Error("Connecting to redis failed")

		return err
	}
	w.fieldLogger.
		WithFields(loggerFields).
		Info("Connected to redis")

	return nil
}

func (w *WorkQueue) createEWMAScheduler() *EWMAScheduler {
	config := EWMASchedulerDefaultConfig.
		Clone().
		Merge(&w.Config.EWMAScheduler).
		Merge(&EWMASchedulerConfig{})

	return NewEWMAScheduler(config)
}

func (w *WorkQueue) createProducer(controller SchedulingController) *Producer {
	config := ProducerDefaultConfig.
		Clone().
		Merge(&w.Config.Producer).
		Merge(&ProducerConfig{
			FileSystemName: w.Config.FileSystemName,
			Namespace:      w.Config.Redis.Namespace,

			SnapshotName: w.Config.SnapshotName,

			Pool:   w.pool,
			DB:     w.Config.DB,
			Logger: w.fieldLogger,

			Controller: w.schedulingController,
		})

	return NewProducer(config)
}

func (w *WorkQueue) createWriteBacker() *WriteBacker {
	config := WriteBackerDefaultConfig.
		Clone().
		Merge(&w.Config.WriteBacker).
		Merge(&WriteBackerConfig{
			FileSystemName: w.Config.FileSystemName,
			Namespace:      w.Config.Redis.Namespace,

			RunId:        w.Config.RunId,
			SnapshotName: w.Config.SnapshotName,

			Pool:   w.pool,
			DB:     w.Config.DB,
			Logger: w.fieldLogger,
		})

	return NewWriteBacker(config)
}

func (w *WorkQueue) createQueueWatcher(productionExhausted <-chan struct{}) *QueueWatcher {
	config := QueueWatcherDefaultConfig.
		Clone().
		Merge(&w.Config.QueueWatcher).
		Merge(&QueueWatcherConfig{
			FileSystemName: w.Config.FileSystemName,
			Namespace:      w.Config.Redis.Namespace,

			RunId:        w.Config.RunId,
			SnapshotName: w.Config.SnapshotName,

			Pool:   w.pool,
			Logger: w.Config.Logger,

			ProductionExhausted: productionExhausted,
		})

	return NewQueueWatcher(config)
}
