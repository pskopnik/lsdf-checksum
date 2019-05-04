package main

import (
	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

var (
	complete           = app.Command("complete", "Complete incomplete runs.")
	completeConfigFile = complete.Flag("config", "Path to the configuration file.").Short('c').PlaceHolder("config.yaml").Required().File()
	completeRun        = complete.Flag("run", "Only complete the specified run.").PlaceHolder("ID").Uint64()
	completeOnlyUntil  = complete.Flag("only-until", "Only complete runs up to and including the specified run.").PlaceHolder("ID").Uint64()
	completeLogLevel   = complete.Flag("log-level", "Log level (severity). All messages with lower severity are omitted.").Default("info").Enum("debug", "info", "warn")
	completeLogFormat  = complete.Flag("log-format", "Format of the log output. All formats print one message per line.").Default("text").Enum("text", "logfmt", "json")
)

func performComplete() error {
	ctx, err := buildMasterContext(MasterContextBuildInputs{
		LogLevel:   *completeLogLevel,
		LogFormat:  *completeLogFormat,
		ConfigFile: *completeConfigFile,
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

	runs, err := compileRunsToContinue(ctx, *completeRun, *completeOnlyUntil)
	if err != nil {
		return err
	}

	for i := range runs {
		err := ctx.RunMasterWithExistingRun(runs[i].ID, meda.RSFinished)
		if err != nil {
			return err
		}
	}

	return nil
}
