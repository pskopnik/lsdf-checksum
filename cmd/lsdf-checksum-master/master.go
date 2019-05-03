package main

import (
	"context"
	"errors"
	"os"
	"os/signal"

	"golang.org/x/sys/unix"

	"github.com/apex/log"

	"git.scc.kit.edu/sdm/lsdf-checksum/master"
	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

var (
	ErrIncompleteRunExists = errors.New("at least one incomplete run exists, this run must be completed / aborted in order to perform this operation")
)

type MasterContext struct {
	context.Context
	cancel     context.CancelFunc
	Logger     log.Interface
	Config     *Config
	DB         *meda.DB
	MedaLocker meda.DBLockLocker
}

func (m *MasterContext) Close() error {
	var err, retErr error

	if m.MedaLocker.IsLocked() {
		err = m.UnlockMeda(m)
		if err != nil && retErr == nil {
			retErr = err
		}
	}

	err = m.DB.Close()
	if err != nil && retErr == nil {
		retErr = err
	}

	if m.cancel != nil {
		m.cancel()
	}

	return retErr
}

func (m *MasterContext) LockMeda(ctx context.Context) error {
	if ctx == nil {
		ctx = m
	}

	if !m.MedaLocker.IsLocked() {
		m.MedaLocker = m.DB.DBLockLocker(m)
	}

	m.Logger.Info("Acquiring lock on meda database")

	err := m.MedaLocker.Lock(ctx)
	if err != nil {
		return err
	}

	m.Logger.Info("Acquired lock on meda database")

	return nil
}

func (m *MasterContext) UnlockMeda(ctx context.Context) error {
	if ctx == nil {
		ctx = m
	}

	m.Logger.Info("Releasing lock on meda database")

	err := m.MedaLocker.Unlock(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (m *MasterContext) RunMasterWithNewRun(syncMode meda.RunSyncMode, targetState meda.RunState) error {
	masterConfig := master.DefaultConfig.
		Clone().
		Merge(&m.Config.Master).
		Merge(&master.Config{
			TargetState: targetState,
			Logger:      m.Logger,
			DB:          m.DB,
		})

	_, master, err := master.NewWithNewRun(m, masterConfig, syncMode)
	if err != nil {
		m.Logger.WithError(err).WithFields(log.Fields{}).
			Error("Encountered error while preparing Master instance")

		return err
	}

	return runMaster(m, master)
}

func (m *MasterContext) RunMasterWithExistingRun(runID uint64, targetState meda.RunState) error {
	masterConfig := master.DefaultConfig.
		Clone().
		Merge(&m.Config.Master).
		Merge(&master.Config{
			TargetState: targetState,
			Logger:      m.Logger,
			DB:          m.DB,
		})

	_, master, err := master.NewWithExistingRun(m, masterConfig, runID)
	if err != nil {
		m.Logger.WithError(err).WithFields(log.Fields{}).
			Error("Encountered error while preparing Master instance")

		return err
	}

	return runMaster(m, master)
}

type MasterContextBuildInputs struct {
	LogLevel            string
	LogFormat           string
	NoOpLogger          bool
	ConfigFile          *os.File
	LeaveConfigFileOpen bool
}

func buildMasterContext(inputs MasterContextBuildInputs) (*MasterContext, error) {
	ctx, cancel := context.WithCancel(context.Background())

	var logger *log.Logger
	if inputs.NoOpLogger {
		logger = prepareNoOpLogger()
	} else {
		logger = prepareLogger(inputs.LogLevel, inputs.LogFormat)
	}

	masterContext := &MasterContext{
		Context: ctx,
		cancel:  cancel,
		Logger:  logger,
	}

	config, err := prepareConfig(inputs.ConfigFile, inputs.LeaveConfigFileOpen, logger)
	if err != nil {
		cancel()
		return nil, err
	}
	masterContext.Config = config

	db, err := prepareDB(ctx, logger, &config.DB)
	if err != nil {
		cancel()
		return nil, err
	}
	masterContext.DB = db

	return masterContext, nil
}

func runMaster(ctx *MasterContext, master *master.Master) error {
	master.Start(ctx)

	go func() {
		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, unix.SIGINT, unix.SIGTERM)
		<-signalChan

		master.SignalStop()
	}()

	err := master.Wait()
	if err != nil {
		ctx.Logger.WithError(err).Error("Encountered error while running Master")

		return err
	}

	return nil
}

func compileRunsToContinue(ctx *MasterContext, runID, onlyUntil uint64) ([]meda.Run, error) {
	var runs []meda.Run

	if runID != 0 {
		run, err := ensureRunIsContinuable(ctx, runID)
		if err != nil {
			return runs, err
		}

		runs = []meda.Run{run}
	} else {
		var err error
		runs, err = ctx.DB.RunsFetchIncomplete(ctx, nil)
		if err != nil {
			ctx.Logger.WithError(err).Error("Encountered error while fetching incomplete runs")
			return runs, err
		}

		if onlyUntil != 0 {
			for i := range runs {
				if runs[i].ID == onlyUntil {
					runs = runs[:i+1]
					break
				}
			}
		}
	}

	return runs, nil
}

func ensureRunIsContinuable(ctx *MasterContext, runID uint64) (meda.Run, error) {
	run, err := ctx.DB.RunsQueryByID(ctx, nil, runID)
	if err != nil {
		ctx.Logger.WithError(err).WithFields(log.Fields{
			"run": runID,
		}).Error("Encountered error while fetching run data")
		return run, err
	}

	incompleteExists, err := ctx.DB.RunsExistsIncompleteBeforeID(ctx, nil, runID)
	if err != nil {
		ctx.Logger.WithError(err).WithFields(log.Fields{
			"run": runID,
		}).Error("Encountered error while checking for incomplete runs")
		return run, err
	}
	if incompleteExists {
		err = ErrIncompleteRunExists
		ctx.Logger.WithError(err).WithFields(log.Fields{
			"run": runID,
		}).Error("Cannot complete the run, there is at least one earlier incomplete run")
		return run, err
	}

	return run, nil
}
