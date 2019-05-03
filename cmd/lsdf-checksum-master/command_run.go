package main

import (
	"github.com/apex/log"

	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

var (
	run           = app.Command("run", "Perform a checksumming run.")
	runConfigFile = run.Flag("config", "Path to the configuration file.").Short('c').PlaceHolder("config.yaml").Required().File()
	runMode       = run.Flag("mode", "Mode of the run.").Default("full").Enum("full", "incremental")
	// runCompletePrevious = run.Flag("complete-previous", "Complete all previous runs.").Default("false").Bool()
	// runAbortPrevious    = run.Flag("abort-previous", "Abort all previous runs before.").Default("false").Bool()
	// runSnapshot         = run.Flag("snapshot", "Snapshot to scan. When this option is specified, snapshot management is disabled for this run.").String()
	runLogLevel  = run.Flag("log-level", "Log level (severity). All messages with lower severity are omitted.").Default("info").Enum("debug", "info", "warn", "error")
	runLogFormat = run.Flag("log-format", "Format of the log output. All formats print one message per line.").Default("text").Enum("text", "logfmt", "json")
)

func performRun() error {
	ctx, err := buildMasterContext(MasterContextBuildInputs{
		LogLevel:   *runLogLevel,
		LogFormat:  *runLogFormat,
		ConfigFile: *runConfigFile,
	})
	if err != nil {
		return err
	}
	defer ctx.Close()

	var syncMode meda.RunSyncMode
	err = syncMode.Scan(*runMode)
	if err != nil {
		ctx.Logger.WithError(err).WithFields(log.Fields{
			"mode": *runMode,
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

	return ctx.RunMasterWithNewRun(syncMode, meda.RSFinished)
}
