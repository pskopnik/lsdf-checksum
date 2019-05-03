package main

import (
	"github.com/apex/log"

	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

var (
	initialiseRun           = app.Command("initialise-run", "Initialise a new run but do not perform any processing. Specifically, the run execution stops as soon as its snapshot has been created in GPFS.")
	initialiseRunConfigFile = initialiseRun.Flag("config", "Path to the configuration file.").Short('c').PlaceHolder("config.yaml").Required().File()
	initialiseRunMode       = initialiseRun.Flag("mode", "Mode of the run.").Default("full").Enum("full", "incremental")
	initialiseRunLogLevel   = initialiseRun.Flag("log-level", "Log level (severity). All messages with lower severity are omitted.").Default("info").Enum("debug", "info", "warn")
	initialiseRunLogFormat  = initialiseRun.Flag("log-format", "Format of the log output. All formats print one message per line.").Default("text").Enum("text", "logfmt", "json")
)

func performInitialiseRun() error {
	ctx, err := buildMasterContext(MasterContextBuildInputs{
		LogLevel:   *initialiseRunLogLevel,
		LogFormat:  *initialiseRunLogFormat,
		ConfigFile: *initialiseRunConfigFile,
	})
	if err != nil {
		return err
	}
	defer ctx.Close()

	var syncMode meda.RunSyncMode
	err = syncMode.Scan(*initialiseRunMode)
	if err != nil {
		ctx.Logger.WithError(err).WithFields(log.Fields{
			"mode": *initialiseRunMode,
		}).Error("Encountered error while parsing supplied run mode")
		return err
	}

	err = ctx.LockMeda(nil)
	if err != nil {
		ctx.Logger.WithError(err).Error("Encountered error while locking meda database")
		return err
	}

	incompleteExists, err := ctx.DB.RunsExistsIncomplete(ctx, nil)
	if err != nil {
		ctx.Logger.WithError(err).Error("Encountered error while checking for incomplete runs")
		return err
	}
	if incompleteExists {
		err = ErrIncompleteRunExists
		ctx.Logger.WithError(err).Error("Cannot perform a new run, there is at least one incomplete run")
		return err
	}

	return ctx.RunMasterWithNewRun(syncMode, meda.RSMedasync)
}
