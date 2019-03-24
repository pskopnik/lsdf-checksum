package master

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/apex/log"
	"github.com/gomodule/redigo/redis"
	pkgErrors "github.com/pkg/errors"
	"gopkg.in/tomb.v2"

	"git.scc.kit.edu/sdm/lsdf-checksum/internal/lifecycle"
	"git.scc.kit.edu/sdm/lsdf-checksum/internal/random"
	"git.scc.kit.edu/sdm/lsdf-checksum/master/medasync"
	"git.scc.kit.edu/sdm/lsdf-checksum/master/workqueue"
	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
	commonRedis "git.scc.kit.edu/sdm/lsdf-checksum/redis"
	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt"
)

const (
	snapshotNameFormat = "lsdf-checksum-master-run-%d-%s"
)

func snapshotName(runId uint64) string {
	return fmt.Sprintf(snapshotNameFormat, runId, random.String(8))
}

//go:generate confions config Config

type Config struct {
	FileSystemName    string
	FileSystemSubpath string

	Run *meda.Run `yaml:"-"`
	// TargetState specifies the RunState which the Master should stop in. There
	// are three states supported: RSFinished, RSAborted and RSMedasync.
	TargetState meda.RunState

	Logger log.Interface `yaml:"-"`
	DB     *meda.DB

	// Redis contains the configuration for the redis.CreatePool function.
	// All known run time dependent options will be overwritten when the final
	// configuration is assembled.
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

	fieldLogger      log.Interface
	pool             *redis.Pool
	poolStored       sync.WaitGroup
	fileSystem       *scaleadpt.FileSystem
	fileSystemStored sync.WaitGroup

	syncer    *medasync.Syncer
	workqueue *workqueue.WorkQueue
}

// New creates and returns a new Master instance using config.
func New(config *Config) *Master {
	return &Master{
		Config: config,
	}
}

// NewWithNewRun creates a new run and returns a Master instance using this run.
// config is cloned and config.Run is replaced with the newly created Run.
func NewWithNewRun(ctx context.Context, config *Config, syncMode meda.RunSyncMode) (*meda.Run, *Master, error) {
	run := &meda.Run{
		SyncMode: syncMode,
		State:    meda.RSInitialised,
	}

	_, err := config.DB.RunsInsertAndSetId(ctx, nil, run)
	if err != nil {
		return nil, nil, pkgErrors.Wrap(err, "NewWithNewRun: insert run")
	}

	config = config.
		Clone().
		Merge(&Config{
			Run: run,
		})

	master := New(config)

	return run, master, nil
}

// NewWithExistingRun fetches an existing run from the database and returns a
// Master instance using this run.
// config is cloned and config.Run is replaced with the retrieved Run.
func NewWithExistingRun(ctx context.Context, config *Config, runId uint64) (*meda.Run, *Master, error) {
	run, err := config.DB.RunsQueryById(ctx, nil, runId)
	if err != nil {
		return nil, nil, pkgErrors.Wrapf(err, "NewWithExistingRun: query run with id = %d", runId)
	}

	config = config.
		Clone().
		Merge(&Config{
			Run: run,
		})

	master := New(config)

	return run, master, nil
}

func (m *Master) Start(ctx context.Context) {
	m.fieldLogger = m.Config.Logger.WithFields(log.Fields{
		"filesystem":   m.Config.FileSystemName,
		"run":          m.Config.Run.Id,
		"sync_mode":    m.Config.Run.SyncMode,
		"target_state": m.Config.TargetState,
		"package":      "master",
		"component":    "Master",
	})

	m.tomb, _ = tomb.WithContext(ctx)

	m.poolStored.Add(1)
	m.fileSystemStored.Add(1)

	m.tomb.Go(func() error {
		m.tomb.Go(m.initialiseRedisPool)

		m.tomb.Go(m.initialiseFileSystem)

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

func (m *Master) initialiseRedisPool() error {
	pool, err := m.createRedisPool()
	if err != nil {
		err = pkgErrors.Wrap(err, "(*Master).initialiseRedisPool")
		m.pool = nil
		m.poolStored.Done()
		return err
	}

	m.pool = pool

	err = m.testRedis()
	if err != nil {
		err = pkgErrors.Wrap(err, "(*Master).initialiseRedisPool")
		m.pool = nil
		m.poolStored.Done()
		return err
	}

	m.poolStored.Done()

	return nil
}

func (m *Master) initialiseFileSystem() error {
	m.fileSystem = m.createFileSystem()
	err := m.testFileSystem()
	if err != nil {
		err = pkgErrors.Wrap(err, "(*Master).initialiseFileSystem")
		m.fileSystem = nil
		m.fileSystemStored.Done()

		m.fieldLogger.WithError(err).WithFields(log.Fields{
			"action":    "stopping",
			"run_state": m.Config.Run.State,
		}).Error("Run is in finished state, cannot abort")

		return err
	}

	m.fileSystemStored.Done()

	return nil
}

func (m *Master) run() error {
	switch m.Config.TargetState {
	case meda.RSMedasync:
		fallthrough
	case meda.RSFinished:
		return m.runFinishing(m.Config.TargetState)
	case meda.RSAborted:
		if m.Config.Run.State == meda.RSFinished {
			m.fieldLogger.WithFields(log.Fields{
				"action":    "skipping",
				"run_state": m.Config.Run.State,
			}).Warn("Cannot abort a finished run, skipping all processing")

			return nil
		}

		return m.runAborting()
	default:
		err := pkgErrors.Wrap(ErrNonSupportedTargetState, "(*Master).run")

		m.fieldLogger.WithError(err).WithFields(log.Fields{
			"action": "stopping",
			// target_state is already set in m.fieldLogger
			"run_state": m.Config.Run.State,
		}).Error("Non supported target state specified")

		return err
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

			m.Config.Run.State = meda.RSSnapshot
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
		case meda.RSAbortingMedasync:
			fallthrough
		case meda.RSAbortingSnapshot:
			fallthrough
		case meda.RSAborted:
			m.fieldLogger.WithFields(log.Fields{
				"action": "continuing",
				// target_state is already set in m.fieldLogger
				"run_state": m.Config.Run.State,
			}).Warn("Run already is in aborting state, ignoring target state and switching to aborting procedure")

			return m.runAborting()
		}

		err = m.updateRun()
		if err != nil {
			err = pkgErrors.Wrap(err, "(*Master).runFinishing")

			m.fieldLogger.WithError(err).WithFields(log.Fields{
				"action":    "stopping",
				"run_state": m.Config.Run.State,
			}).Error("Encountered error while updating run in database")

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
			err = pkgErrors.Wrap(ErrFinishedNotAbortable, "(*Master).runAborting")

			m.fieldLogger.WithError(err).WithFields(log.Fields{
				"action":    "stopping",
				"run_state": m.Config.Run.State,
			}).Error("Run is in finished state, cannot abort")

			return err
		}

		err = m.updateRun()
		if err != nil {
			err = pkgErrors.Wrap(err, "(*Master).runAborting")

			m.fieldLogger.WithError(err).WithFields(log.Fields{
				"action":    "stopping",
				"run_state": m.Config.Run.State,
			}).Error("Encountered error while updating run in database")

			return err
		}
	}
}

func (m *Master) updateRun() error {
	ctx, cancel := context.WithCancel(m.tomb.Context(nil))
	defer cancel()

	tx, err := m.Config.DB.BeginTxx(ctx, nil)
	if err != nil {
		return pkgErrors.Wrap(err, "(*Master).updateRun: begin transaction")
	}

	result, err := m.Config.DB.RunsUpdate(ctx, tx, m.Config.Run)
	if err != nil {
		_ = tx.Rollback()
		return pkgErrors.Wrap(err, "(*Master).updateRun: perform update")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		_ = tx.Rollback()
		return pkgErrors.Wrap(err, "(*Master).updateRun: retrieve number of rows affected")
	}

	if rowsAffected != 1 {
		_ = tx.Rollback()
		err = pkgErrors.Wrapf(ErrRunUpdateFailed, "(*Master).updateRun: unexpected number of rows affected = %d", rowsAffected)

		m.fieldLogger.WithError(err).WithFields(log.Fields{
			"action":        "escalating",
			"rows_affected": rowsAffected,
		}).Error("More than 1 row affected by update run statement")

		return err
	}

	err = tx.Commit()
	if err != nil {
		return pkgErrors.Wrap(err, "(*Master).updateRun: committing transaction")
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
		err := pkgErrors.Wrap(ErrFileSystemNil, "(*Master).ensureFileSystem")

		m.fieldLogger.WithError(err).WithFields(log.Fields{
			"action": "escalating",
		}).Error("File system is nil but tomb is not dying (yet)")

		return nil, err
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
		err := pkgErrors.Wrap(ErrPoolNil, "(*Master).ensurePool")

		m.fieldLogger.WithError(err).WithFields(log.Fields{
			"action": "escalating",
		}).Error("Pool is nil but tomb is not dying (yet)")

		return nil, err
	}

	return m.pool, nil
}

func (m *Master) runSnapshotState() error {
	if _, err := m.ensureFileSystem(); err != nil {
		err = pkgErrors.Wrap(err, "(*Master).runSnapshotState")

		m.fieldLogger.WithError(err).WithFields(log.Fields{
			"action": "stopping",
		}).Error("Encountered error while retrieving the file system")

		return err
	}

	if !m.Config.Run.SnapshotName.Valid {
		err := pkgErrors.Wrap(ErrMissingSnapshotName, "(*Master).runSnapshotState")

		m.fieldLogger.WithError(err).WithFields(log.Fields{
			"action":    "stopping",
			"run_state": m.Config.Run.State,
		}).Error("Cannot perform snapshot state processing, run is missing SnapshotName value")

		return err
	}

	m.fieldLogger.WithFields(log.Fields{
		"snapshot":  m.Config.Run.SnapshotName.String,
		"run_state": m.Config.Run.State,
	}).Info("Creating snapshot in Spectrum Scale file system")

	snapshot, err := m.fileSystem.CreateSnapshot(m.Config.Run.SnapshotName.String)
	if err == scaleadpt.ErrSnapshotAlreadyExists {

		m.fieldLogger.WithFields(log.Fields{
			"snapshot":  m.Config.Run.SnapshotName.String,
			"run_state": m.Config.Run.State,
		}).Info("Snapshot already exists, retrieving data instead of creating")

		snapshot, err = m.fileSystem.GetSnapshot(m.Config.Run.SnapshotName.String)
		if err != nil {
			err = pkgErrors.Wrap(err, "(*Master).runSnapshotState: GetSnapshot from file system")

			m.fieldLogger.WithError(err).WithFields(log.Fields{
				"action":    "stopping",
				"run_state": m.Config.Run.State,
			}).Error("Encountered error while getting snapshot data")

			return err
		}
	} else if err != nil {
		err = pkgErrors.Wrap(err, "(*Master).runSnapshotState: CreateSnapshot in file system")

		m.fieldLogger.WithError(err).WithFields(log.Fields{
			"action":    "stopping",
			"run_state": m.Config.Run.State,
		}).Error("Encountered error while creating snapshot")

		return err
	} else {
		m.fieldLogger.WithFields(log.Fields{
			"snapshot":  m.Config.Run.SnapshotName.String,
			"run_state": m.Config.Run.State,
		}).Info("Created snapshot in Spectrum Scale file system")
	}

	m.fieldLogger.WithFields(log.Fields{
		"run_state":           m.Config.Run.State,
		"snapshot":            m.Config.Run.SnapshotName.String,
		"snapshot_id":         snapshot.Id,
		"snapshot_created_at": snapshot.CreatedAt,
	}).Info("Retrieved snapshot data")

	m.Config.Run.SnapshotId.Uint64 = uint64(snapshot.Id)
	m.Config.Run.SnapshotId.Valid = true
	m.Config.Run.RunAt.Time = snapshot.CreatedAt
	m.Config.Run.RunAt.Valid = true

	return nil
}

func (m *Master) runMedasyncState() error {
	syncer, err := m.createSyncer()
	if err != nil {
		err = pkgErrors.Wrap(err, "(*Master).runMedasyncState")

		m.fieldLogger.WithError(err).WithFields(log.Fields{
			"action":    "stopping",
			"run_state": m.Config.Run.State,
		}).Error("Encountered error while creating Syncer instance")

		return err
	}

	m.fieldLogger.Info("Starting medasync")

	ctx, cancel := context.WithCancel(m.tomb.Context(nil))
	err = syncer.Run(ctx)
	cancel()
	if err != nil {
		err = pkgErrors.Wrap(err, "(*Master).runMedasyncState: run Syncer")

		m.fieldLogger.WithError(err).WithFields(log.Fields{
			"action":    "stopping",
			"run_state": m.Config.Run.State,
		}).Error("Encountered error while performing medasync")

		return err
	}

	m.fieldLogger.Info("Finished medasync")

	return nil
}

func (m *Master) runWorkqueueState() error {
	workQueue, err := m.createWorkQueue()
	if err != nil {
		err = pkgErrors.Wrap(err, "(*Master).runWorkqueueState")

		m.fieldLogger.WithError(err).WithFields(log.Fields{
			"action":    "stopping",
			"run_state": m.Config.Run.State,
		}).Error("Encountered error while creating WorkQueue instance")

		return err
	}

	m.fieldLogger.WithFields(log.Fields{
		"run_state": m.Config.Run.State,
	}).Info("Starting workqueue")

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
			err = pkgErrors.Wrap(err, "(*Master).runWorkqueueState: run WorkQueue")

			m.fieldLogger.WithError(err).WithFields(log.Fields{
				"action":    "stopping",
				"run_state": m.Config.Run.State,
			}).Error("Encountered error while performing workqueue")
		} else {
			m.fieldLogger.WithFields(log.Fields{
				"run_state": m.Config.Run.State,
			}).Info("Finished workqueue")
		}
	}

	return err
}

func (m *Master) runCleanupState() error {
	if _, err := m.ensureFileSystem(); err != nil {
		err = pkgErrors.Wrap(err, "(*Master).runCleanupState")

		m.fieldLogger.WithError(err).WithFields(log.Fields{
			"action": "stopping",
		}).Error("Encountered error while retrieving the file system")

		return err
	}

	if !m.Config.Run.SnapshotName.Valid {
		err := pkgErrors.Wrap(ErrMissingSnapshotName, "(*Master).runCleanupState")

		m.fieldLogger.WithError(err).WithFields(log.Fields{
			"action":    "stopping",
			"run_state": m.Config.Run.State,
		}).Error("Cannot perform snapshot deletion, run is missing SnapshotName value")

		return err
	}

	m.fieldLogger.WithFields(log.Fields{
		"snapshot":  m.Config.Run.SnapshotName.String,
		"run_state": m.Config.Run.State,
	}).Info("Deleting snapshot in Spectrum Scale file system")

	err := m.fileSystem.DeleteSnapshot(m.Config.Run.SnapshotName.String)
	if err == scaleadpt.ErrSnapshotDoesNotExist {
		m.fieldLogger.WithFields(log.Fields{
			"snapshot":  m.Config.Run.SnapshotName.String,
			"run_state": m.Config.Run.State,
		}).Info("Snapshot does not exists, presuming that it has already been deleted")
	} else if err != nil {
		err = pkgErrors.Wrap(err, "(*Master).runCleanupState: DeleteSnapshot in file system")

		m.fieldLogger.WithError(err).WithFields(log.Fields{
			"action":    "stopping",
			"run_state": m.Config.Run.State,
		}).Error("Encountered error while deleting snapshot")

		return err
	} else {
		m.fieldLogger.WithFields(log.Fields{
			"snapshot":  m.Config.Run.SnapshotName.String,
			"run_state": m.Config.Run.State,
		}).Info("Deleted snapshot in Spectrum Scale file system")
	}

	return nil
}

func (m *Master) runAbortingMedasyncState() error {
	syncer, err := m.createSyncer()
	if err != nil {
		err = pkgErrors.Wrap(err, "(*Master).runAbortingMedasyncState")

		m.fieldLogger.WithError(err).WithFields(log.Fields{
			"action":    "stopping",
			"run_state": m.Config.Run.State,
		}).Error("Encountered error while creating Syncer instance")

		return err
	}

	m.fieldLogger.WithFields(log.Fields{
		"run_state": m.Config.Run.State,
	}).Info("Starting running CleanUp on Syncer")

	ctx, cancel := context.WithCancel(m.tomb.Context(nil))
	err = syncer.CleanUp(ctx)
	cancel()
	if err != nil {
		err = pkgErrors.Wrap(err, "(*Master).runAbortingMedasyncState: run CleanUp on Syncer")

		m.fieldLogger.WithError(err).WithFields(log.Fields{
			"action":    "stopping",
			"run_state": m.Config.Run.State,
		}).Error("Encountered error while performing CleanUp on Syncer")

		return err
	}

	m.fieldLogger.WithFields(log.Fields{
		"run_state": m.Config.Run.State,
	}).Info("Finished running CleanUp on Syncer")

	return nil
}

func (m *Master) runAbortingSnapshotState() error {
	m.fieldLogger.WithFields(log.Fields{
		"run_state": m.Config.Run.State,
	}).Info("Starting running cleanup state procedure")

	err := m.runCleanupState()
	if err != nil {
		err = pkgErrors.Wrap(err, "(*Master).runAbortingSnapshotState")

		m.fieldLogger.WithError(err).WithFields(log.Fields{
			"action":    "stopping",
			"run_state": m.Config.Run.State,
		}).Error("Encountered error while running the cleanup state procedure")

		return err
	}

	m.fieldLogger.WithFields(log.Fields{
		"run_state": m.Config.Run.State,
	}).Info("Finished running cleanup state procedure")

	return nil
}

func (m *Master) createFileSystem() *scaleadpt.FileSystem {
	return scaleadpt.OpenFileSystem(m.Config.FileSystemName)
}

func (m *Master) testFileSystem() error {
	loggerFields := log.Fields{
		"filesystem": m.fileSystem.GetName(),
	}

	version, err := m.fileSystem.GetVersion()
	if err != nil {
		err = pkgErrors.Wrap(err, "(*Master).testFileSystem: GetVersion from file system")

		m.fieldLogger.WithError(err).WithFields(loggerFields).
			Error("Spectrum Scale file system GetVersion returned error")

		return err
	}

	m.fieldLogger.WithFields(loggerFields).WithFields(log.Fields{
		"version": version,
	}).Info("Retrieved version from Spectrum Scale file system")

	return nil
}

func (m *Master) createRedisPool() (*redis.Pool, error) {
	config := commonRedis.DefaultConfig.
		Clone().
		Merge(&m.Config.Redis).
		Merge(&commonRedis.Config{})

	pool, err := commonRedis.CreatePool(config)
	if err != nil {
		return nil, pkgErrors.Wrap(err, "(*Master).createRedisPool")
	}

	return pool, nil
}

func (m *Master) testRedis() error {
	loggerFields := log.Fields{
		"network":       m.Config.Redis.Network,
		"address":       m.Config.Redis.Address,
		"database":      m.Config.Redis.Database,
		"authenticated": len(m.Config.Redis.Password) > 0,
	}

	err := commonRedis.TestPool(m.tomb.Context(nil), m.pool)
	if err != nil {
		err = pkgErrors.Wrap(err, "(*Master).testRedis")

		m.fieldLogger.WithError(err).WithFields(loggerFields).
			Error("Testing redis connection pool failed")

		return err
	}

	m.fieldLogger.WithFields(loggerFields).
		Info("Testing redis connection pool succeeded")

	return nil
}

func (m *Master) createSyncer() (*medasync.Syncer, error) {
	if _, err := m.ensureFileSystem(); err != nil {
		return nil, pkgErrors.Wrap(err, "(*Master).createSyncer")
	}

	if !m.Config.Run.SnapshotName.Valid {
		return nil, pkgErrors.Wrap(ErrMissingSnapshotName, "(*Master).createSyncer")
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
			Logger:       m.Config.Logger,
		})

	return medasync.New(config), nil
}

func (m *Master) createWorkQueue() (*workqueue.WorkQueue, error) {
	if _, err := m.ensureFileSystem(); err != nil {
		return nil, pkgErrors.Wrap(err, "(*Master).createWorkQueue")
	}

	if !m.Config.Run.SnapshotName.Valid {
		return nil, pkgErrors.Wrap(ErrMissingSnapshotName, "(*Master).createWorkQueue")
	}

	if _, err := m.ensurePool(); err != nil {
		return nil, pkgErrors.Wrap(err, "(*Master).createWorkQueue")
	}

	config := workqueue.DefaultConfig.
		Clone().
		Merge(&m.Config.WorkQueue).
		Merge(&workqueue.Config{
			FileSystemName: m.fileSystem.GetName(),
			RunId:          m.Config.Run.Id,
			SnapshotName:   m.Config.Run.SnapshotName.String,
			DB:             m.Config.DB,
			Logger:         m.Config.Logger,
			Pool:           m.pool,
		})

	return workqueue.New(config), nil
}
