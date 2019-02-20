// Package workqueue
package workqueue

import (
	"context"
	"errors"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/pskopnik/rewledis"
	"github.com/sirupsen/logrus"
	"gopkg.in/tomb.v2"

	"git.scc.kit.edu/sdm/lsdf-checksum/internal/lifecycle"
	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

const gocraftWorkNamespaceBase string = "lsdf-checksum/workqueue:work"

func GocraftWorkNamespace(prefix string) string {
	return prefix + gocraftWorkNamespaceBase
}

func PerformanceMonitorUnit(fileSystemName, snapshotName string) string {
	return fileSystemName + "-" + snapshotName
}

var _ GetNodesNumer = queueSchedulerGetNodesNumer{}

type queueSchedulerGetNodesNumer struct {
	*QueueScheduler
}

func (q queueSchedulerGetNodesNumer) GetNodesNum() (uint, error) {
	return q.GetNodesNum()
}

// Error variables related to WorkQueue.
var (
	ErrUnsupportedRedisDialect = errors.New("specified redis dialect is not supported")
)

//go:generate confions config RedisConfig

// RedisConfig contains configuration options for a connection pool to a redis
// database.
type RedisConfig struct {
	Dialect           string
	Network           string
	Address           string
	Database          int
	Prefix            string
	Password          string
	MaxIdle           int
	IdleTimeout       time.Duration
	InternalMaxActive int
}

var RedisDefaultConfig = &RedisConfig{
	Dialect:     "redis",
	MaxIdle:     10,
	IdleTimeout: 300 * time.Second,
}

//go:generate confions config Config

type Config struct {
	FileSystemName string

	RunId        uint64
	SnapshotName string

	DB     *meda.DB           `yaml:"-"`
	Logger logrus.FieldLogger `yaml:"-"`

	Redis RedisConfig
	// EWMAScheduler contains the configuration for the EWMAScheduler
	// SchedulingController. Here only static configuration options should be
	// set.
	// All known run time dependent options (database connections, RunId, etc.)
	// will be overwritten when the final configuration is assembled.
	EWMAScheduler EWMASchedulerConfig
	// Producer contains the configuration for the Producer. Here only static
	// configuration options should be set.
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
	// PerformanceMonitor contains the configuration for the PerformanceMonitor.
	// Here only static configuration options should be set.
	// All known run time dependent options (database connections, RunId, etc.)
	// will be overwritten when the final configuration is assembled.
	PerformanceMonitor PerformanceMonitorConfig
}

var DefaultConfig = &Config{
	Redis: *RedisDefaultConfig,
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
	performanceMonitor   *PerformanceMonitor
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

	w.tomb, _ = tomb.WithContext(ctx)

	w.tomb.Go(func() error {
		pool, err := w.createRedisPool()
		if err != nil {
			return err
		}
		w.pool = pool

		err = w.testRedis()
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

		w.performanceMonitor = w.createPerformanceMonitor(w.producer.queueScheduler)
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

func (w *WorkQueue) createRedisPool() (*redis.Pool, error) {
	config := RedisDefaultConfig.
		Clone().
		Merge(&w.Config.Redis).
		Merge(&RedisConfig{})

	if config.Dialect == "redis" {
		return &redis.Pool{
			Dial: func() (redis.Conn, error) {
				return redis.Dial(
					config.Network,
					config.Address,
					redis.DialDatabase(config.Database),
					redis.DialPassword(config.Password),
				)
			},
			MaxIdle:     config.MaxIdle,
			IdleTimeout: config.IdleTimeout,
		}, nil
	} else if config.Dialect == "ledis" {
		return rewledis.NewPool(&rewledis.PoolConfig{
			Dial: func() (redis.Conn, error) {
				return redis.Dial(
					config.Network,
					config.Address,
					redis.DialDatabase(config.Database),
					redis.DialPassword(config.Password),
				)
			},
			MaxIdle:     config.MaxIdle,
			IdleTimeout: config.IdleTimeout,
		}, config.InternalMaxActive), nil
	} else {
		return nil, ErrUnsupportedRedisDialect
	}
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
			Namespace:      GocraftWorkNamespace(w.Config.Redis.Prefix),

			SnapshotName: w.Config.SnapshotName,

			Pool:   w.pool,
			DB:     w.Config.DB,
			Logger: w.fieldLogger,

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
			Namespace:      GocraftWorkNamespace(w.Config.Redis.Prefix),

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
			Namespace:      GocraftWorkNamespace(w.Config.Redis.Prefix),

			RunId:        w.Config.RunId,
			SnapshotName: w.Config.SnapshotName,

			Pool:   w.pool,
			Logger: w.Config.Logger,

			ProductionExhausted: productionExhausted,
		})

	return NewQueueWatcher(config)
}

func (w *WorkQueue) createPerformanceMonitor(queueScheduler *QueueScheduler) *PerformanceMonitor {

	config := PerformanceMonitorDefaultConfig.
		Clone().
		Merge(&w.Config.PerformanceMonitor).
		Merge(&PerformanceMonitorConfig{
			Prefix: w.Config.Redis.Prefix,

			Unit: PerformanceMonitorUnit(w.Config.FileSystemName, w.Config.SnapshotName),

			Pool:   w.pool,
			Logger: w.Config.Logger,
			GetNodesNumer: queueSchedulerGetNodesNumer{
				QueueScheduler: queueScheduler,
			},
		})

	return NewPerformanceMonitor(config)
}
