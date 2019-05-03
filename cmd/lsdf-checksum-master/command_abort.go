package main

import (
	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

var (
	abort           = app.Command("abort", "Abort incomplete runs.")
	abortConfigFile = abort.Flag("config", "Path to the configuration file.").Short('c').PlaceHolder("config.yaml").Required().File()
	abortRun        = abort.Flag("run", "Only abort the specified run.").PlaceHolder("ID").Uint64()
	abortOnlyUntil  = abort.Flag("only-until", "Only abort runs up to and including the specified run.").PlaceHolder("ID").Uint64()
	abortLogLevel   = abort.Flag("log-level", "Log level (severity). All messages with lower severity are omitted.").Default("info").Enum("debug", "info", "warn")
	abortLogFormat  = abort.Flag("log-format", "Format of the log output. All formats print one message per line.").Default("text").Enum("text", "logfmt", "json")
)

func performAbort() error {
	ctx, err := buildMasterContext(MasterContextBuildInputs{
		LogLevel:   *abortLogLevel,
		LogFormat:  *abortLogFormat,
		ConfigFile: *abortConfigFile,
	})
	if err != nil {
		return err
	}
	defer ctx.Close()

	err = ctx.LockMeda(nil)
	if err != nil {
		ctx.Logger.WithError(err).Error("Encountered error while locking meda database")
		return err
	}

	runs, err := compileRunsToContinue(ctx, *abortRun, *abortOnlyUntil)
	if err != nil {
		return err
	}

	for i := range runs {
		err := ctx.RunMasterWithExistingRun(runs[i].ID, meda.RSAborted)
		if err != nil {
			return err
		}
	}

	return nil
}
