package master

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
	"gopkg.in/tomb.v2"

	"git.scc.kit.edu/sdm/lsdf-checksum/internal/lifecycle"
	"git.scc.kit.edu/sdm/lsdf-checksum/master/medasync"
	"git.scc.kit.edu/sdm/lsdf-checksum/master/workqueue"
	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
	commonRedis "git.scc.kit.edu/sdm/lsdf-checksum/redis"
	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt"
)

const (
	snapshotNameFormat = "lsdf-checksum-master-run-%d-%d"
)

func snapshotName(runId uint64) string {
	now := time.Now()
	return fmt.Sprintf(snapshotNameFormat, runId, now.Nanosecond())
}

//go:generate confions config Config

type Config struct {
	FileSystemName    string
	FileSystemSubpath string

	Run *meda.Run `yaml:"-"`
	// TargetState specifies the RunState which the Master should stop in. There
	// are three states supported: RSFinished, RSAborted and RSMedasync.
	TargetState meda.RunState

	Logger logrus.FieldLogger `yaml:"-"`
	DB     *meda.DB

	Redis commonRedis.Config
	// MedaSync contains the configuration for the medasync.Syncer. Here only
	// static configuration options should be set.
	// All known run time dependent options (database connections, RunId, etc.)
	// will be overwritten when the final configuration is assembled.
	MedaSync medasync.Config
	// WorkQueue contains the configuration for the workqueue.WorkQueue. Here
	// only static configuration options should be set.
	// All known run time dependent options (database connections, RunId, etc.)
	// will be overwritten when the final configuration is assembled.
	WorkQueue workqueue.Config
}

var DefaultConfig = &Config{}

// Error variables related to Master.
var (
	ErrNonSupportedTargetState = errors.New("the passed target state is not supported")
	ErrFinishedNotAbortable    = errors.New("a run in the Finished state is no longer abortable")
	ErrMissingSnapshotName     = errors.New("inconsistent run data: snapshot name is missing")
	ErrRunUpdateFailed         = errors.New("updating run data failed")
	ErrFileSystemNil           = errors.New("file system is nil")
	ErrPoolNil                 = errors.New("pool is nil")
)

type Master struct {
	Config *Config

	tomb *tomb.Tomb

	fieldLogger      logrus.FieldLogger
	pool             *redis.Pool
	poolStored       sync.WaitGroup
	fileSystem       *scaleadpt.FileSystem
	fileSystemStored sync.WaitGroup

	syncer    *medasync.Syncer
	workqueue *workqueue.WorkQueue
}

func New(config *Config) *Master {
	return &Master{
		Config: config,
	}
}

func (m *Master) Start(ctx context.Context) {
	m.fieldLogger = m.Config.Logger.WithFields(logrus.Fields{
		"filesystem": m.Config.FileSystemName,
		"run":        m.Config.Run.Id,
		"package":    "master",
		"component":  "Master",
	})

	m.tomb, _ = tomb.WithContext(ctx)

	m.poolStored.Add(1)
	m.fileSystemStored.Add(1)

	m.tomb.Go(func() error {
		m.tomb.Go(func() error {
			pool, err := m.createRedisPool()
			if err != nil {
				m.pool = nil
				m.poolStored.Done()
				return err
			}

			m.pool = pool

			err = m.testRedis()
			if err != nil {
				m.pool = nil
				m.poolStored.Done()
				return err
			}

			m.poolStored.Done()

			return nil
		})

		m.tomb.Go(func() error {
			m.fileSystem = m.createFileSystem()
			err := m.testFileSystem()
			if err != nil {
				m.fileSystem = nil
				m.fileSystemStored.Done()
				return err
			}

			m.fileSystemStored.Done()

			return nil
		})

		m.tomb.Go(m.run)

		return nil
	})
}

func (m *Master) SignalStop() {
	m.tomb.Kill(lifecycle.ErrStopSignalled)
}

func (m *Master) Wait() error {
	return m.tomb.Wait()
}

func (m *Master) Dead() <-chan struct{} {
	return m.tomb.Dead()
}

func (m *Master) Err() error {
	return m.tomb.Err()
}

func (m *Master) run() error {
	switch m.Config.TargetState {
	case meda.RSMedasync:
		fallthrough
	case meda.RSFinished:
		return m.runFinishing(m.Config.TargetState)
	case meda.RSAborted:
		if m.Config.Run.State == meda.RSFinished {
			// TODO log warning
			// log
			return nil
		}

		return m.runAborting()
	default:
		return ErrNonSupportedTargetState
	}
}

func (m *Master) runFinishing(targetState meda.RunState) error {
	var err error

	for {
		if m.Config.Run.State == targetState {
			return nil
		}

		switch m.Config.Run.State {
		case meda.RSInitialised:
			err = m.runInitialisedState()
			if err != nil {
				return err
			}

			m.Config.Run.State = meda.RSMedasync
		case meda.RSSnapshot:
			err = m.runSnapshotState()
			if err != nil {
				return err
			}

			m.Config.Run.State = meda.RSMedasync
		case meda.RSMedasync:
			err = m.runMedasyncState()
			if err != nil {
				return err
			}

			m.Config.Run.State = meda.RSWorkqueue
		case meda.RSWorkqueue:
			err = m.runWorkqueueState()
			if err != nil {
				return err
			}

			m.Config.Run.State = meda.RSCleanup
		case meda.RSCleanup:
			err = m.runCleanupState()
			if err != nil {
				return err
			}

			m.Config.Run.State = meda.RSFinished
		case meda.RSFinished:
			return nil
		}

		err = m.updateRun()
		if err != nil {
			return err
		}
	}
}

func (m *Master) runAborting() error {
	var err error

	for {
		switch m.Config.Run.State {
		case meda.RSInitialised:
			m.Config.Run.State = meda.RSAborted
		case meda.RSSnapshot:
			m.Config.Run.State = meda.RSAbortingSnapshot
		case meda.RSMedasync:
			m.Config.Run.State = meda.RSAbortingMedasync
		case meda.RSWorkqueue:
			m.Config.Run.State = meda.RSAbortingSnapshot
		case meda.RSCleanup:
			m.Config.Run.State = meda.RSAbortingSnapshot
		case meda.RSAbortingMedasync:
			err = m.runAbortingMedasyncState()
			if err != nil {
				return err
			}

			m.Config.Run.State = meda.RSAbortingSnapshot
		case meda.RSAbortingSnapshot:
			err = m.runAbortingSnapshotState()
			if err != nil {
				return err
			}

			m.Config.Run.State = meda.RSAborted
		case meda.RSAborted:
			return nil
		case meda.RSFinished:
			return ErrFinishedNotAbortable
		}

		err = m.updateRun()
		if err != nil {
			return err
		}
	}
}

func (m *Master) updateRun() error {
	ctx, cancel := context.WithCancel(m.tomb.Context(nil))
	defer cancel()

	tx, err := m.Config.DB.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}

	result, err := m.Config.DB.RunsUpdate(ctx, tx, m.Config.Run)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	if rowsAffected != 1 {
		_ = tx.Rollback()
		// Log, escalating
		return ErrRunUpdateFailed
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (m *Master) runInitialisedState() error {
	m.Config.Run.SnapshotName.String = snapshotName(m.Config.Run.Id)
	m.Config.Run.SnapshotName.Valid = true
	return nil
}

func (m *Master) ensureFileSystem() (*scaleadpt.FileSystem, error) {
	m.fileSystemStored.Wait()

	select {
	case <-m.tomb.Dying():
		return nil, tomb.ErrDying
	default:
	}

	if m.fileSystem == nil {
		// log?
		return nil, ErrFileSystemNil
	}

	return m.fileSystem, nil
}

func (m *Master) ensurePool() (*redis.Pool, error) {
	m.poolStored.Wait()

	select {
	case <-m.tomb.Dying():
		return nil, tomb.ErrDying
	default:
	}

	if m.pool == nil {
		// log?
		return nil, ErrPoolNil
	}

	return m.pool, nil
}

func (m *Master) runSnapshotState() error {
	if _, err := m.ensureFileSystem(); err != nil {
		return err
	}

	if !m.Config.Run.SnapshotName.Valid {
		// log
		return ErrMissingSnapshotName
	}

	snapshot, err := m.fileSystem.CreateSnapshot(m.Config.Run.SnapshotName.String)
	if err == scaleadpt.ErrSnapshotAlreadyExists {
		// log
		snapshot, err = m.fileSystem.GetSnapshot(m.Config.Run.SnapshotName.String)
		if err != nil {
			// log
			return err
		}
	} else if err != nil {
		// log
		return err
	}

	m.Config.Run.SnapshotId.Uint64 = uint64(snapshot.Id)
	m.Config.Run.SnapshotId.Valid = true
	m.Config.Run.RunAt.Time = snapshot.CreatedAt
	m.Config.Run.RunAt.Valid = true

	return nil
}

func (m *Master) runMedasyncState() error {
	syncer, err := m.createSyncer()
	if err != nil {
		// log
		return err
	}

	ctx, cancel := context.WithCancel(m.tomb.Context(nil))
	err = syncer.Run(ctx)
	cancel()
	if err != nil {
		// log
		return err
	}

	return nil
}

func (m *Master) runWorkqueueState() error {
	workQueue, err := m.createWorkQueue()
	if err != nil {
		// log
		return err
	}

	ctx, cancel := context.WithCancel(m.tomb.Context(nil))
	workQueue.Start(ctx)

	select {
	case <-m.tomb.Dying():
		err = tomb.ErrDying
		cancel()
		_ = workQueue.Wait()
	case <-workQueue.Dead():
		err = workQueue.Err()
		cancel()
		if err != nil {
			// TODO should errors be filtered out? lifecycle.ErrStopSignalled, context.Canceled
			// log
		}
	}

	return err
}

func (m *Master) runCleanupState() error {
	if _, err := m.ensureFileSystem(); err != nil {
		// log
		return err
	}

	if !m.Config.Run.SnapshotName.Valid {
		// log
		return ErrMissingSnapshotName
	}

	err := m.fileSystem.DeleteSnapshot(m.Config.Run.SnapshotName.String)
	if err == scaleadpt.ErrSnapshotDoesNotExist {
		// log
	} else if err != nil {
		// log
		return err
	}

	return nil
}

func (m *Master) runAbortingMedasyncState() error {
	syncer, err := m.createSyncer()
	if err != nil {
		// log
		return err
	}

	ctx, cancel := context.WithCancel(m.tomb.Context(nil))
	err = syncer.CleanUp(ctx)
	cancel()
	if err != nil {
		// log
		return err
	}

	return nil
}

func (m *Master) runAbortingSnapshotState() error {
	return m.runCleanupState()
}

func (m *Master) createFileSystem() *scaleadpt.FileSystem {
	return scaleadpt.OpenFileSystem(m.Config.FileSystemName)
}

func (m *Master) testFileSystem() error {
	loggerFields := logrus.Fields{
		"filesystem": m.fileSystem.GetName(),
	}

	version, err := m.fileSystem.GetVersion()
	if err != nil {
		m.fieldLogger.
			WithError(err).
			WithFields(loggerFields).
			Error("Spectrum Scale file system GetVersion returned error")

		return err
	}

	m.fieldLogger.
		WithFields(loggerFields).
		WithFields(logrus.Fields{
			"version": version,
		}).
		Info("Opened Spectrum Scale file system")

	return nil
}

func (m *Master) createRedisPool() (*redis.Pool, error) {
	config := commonRedis.DefaultConfig.
		Clone().
		Merge(&m.Config.Redis).
		Merge(&commonRedis.Config{})

	pool, err := commonRedis.CreatePool(config)
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func (m *Master) testRedis() error {
	loggerFields := logrus.Fields{
		"network":       m.Config.Redis.Network,
		"address":       m.Config.Redis.Address,
		"database":      m.Config.Redis.Database,
		"authenticated": len(m.Config.Redis.Password) > 0,
	}

	err := commonRedis.TestPool(m.tomb.Context(nil), m.pool)
	if err != nil {
		m.fieldLogger.
			WithFields(loggerFields).
			WithError(err).
			Error("Testing redis connection pool failed")

		return err
	}

	m.fieldLogger.
		WithFields(loggerFields).
		Info("Testing redis connection pool succeeded")

	return nil
}

func (m *Master) createSyncer() (*medasync.Syncer, error) {
	if _, err := m.ensureFileSystem(); err != nil {
		return nil, err
	}

	if !m.Config.Run.SnapshotName.Valid {
		return nil, ErrMissingSnapshotName
	}

	config := medasync.DefaultConfig.
		Clone().
		Merge(&m.Config.MedaSync).
		Merge(&medasync.Config{
			Subpath:      m.Config.FileSystemSubpath,
			SnapshotName: m.Config.Run.SnapshotName.String,
			RunId:        m.Config.Run.Id,
			SyncMode:     m.Config.Run.SyncMode,
			DB:           m.Config.DB,
			FileSystem:   m.fileSystem,
			Logger:       m.fieldLogger,
		})

	return medasync.New(config), nil
}

func (m *Master) createWorkQueue() (*workqueue.WorkQueue, error) {
	if _, err := m.ensureFileSystem(); err != nil {
		return nil, err
	}

	if !m.Config.Run.SnapshotName.Valid {
		return nil, ErrMissingSnapshotName
	}

	if _, err := m.ensurePool(); err != nil {
		return nil, err
	}

	config := workqueue.DefaultConfig.
		Clone().
		Merge(&m.Config.WorkQueue).
		Merge(&workqueue.Config{
			FileSystemName: m.fileSystem.GetName(),
			RunId:          m.Config.Run.Id,
			SnapshotName:   m.Config.Run.SnapshotName.String,
			DB:             m.Config.DB,
			Logger:         m.fieldLogger,
			Pool:           m.pool,
		})

	return workqueue.New(config), nil
}
