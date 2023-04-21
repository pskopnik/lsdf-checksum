package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"golang.org/x/sys/unix"

	"github.com/alecthomas/kingpin/v2"
	"github.com/apex/log"
	"github.com/apex/log/handlers/json"
	"github.com/apex/log/handlers/logfmt"
	"github.com/apex/log/handlers/text"
	"gopkg.in/yaml.v3"

	"git.scc.kit.edu/sdm/lsdf-checksum/worker"
)

type YAMLConfig struct {
	Worker worker.Config
}

var (
	app = kingpin.New("lsdf-checksum-worker", "Worker command of the lsdf-checksum system.")

	run           = app.Command("run", "Run the worker.")
	runConfigFile = run.Flag("config", "Path to the configuration file.").Short('c').PlaceHolder("config.yaml").Required().File()
	runLogLevel   = run.Flag("log-level", "Log level (severity). All messages with lower severity are omitted.").Default("info").Enum("debug", "info", "warn", "error")
	runLogFormat  = run.Flag("log-format", "Format of the log output. All formats print one message per line.").Default("text").Enum("text", "logfmt", "json")
)

func main() {
	var err error

	switch kingpin.MustParse(app.Parse(os.Args[1:])) {
	case run.FullCommand():
		err = runWorker()
	}

	if err != nil {
		os.Exit(1)
	}
}

func runWorker() error {
	logger := prepareLogger(*runLogLevel, *runLogFormat)

	config, err := readConfig(*runConfigFile)
	if err != nil {
		_ = (*runConfigFile).Close()

		logger.WithError(err).WithFields(log.Fields{
			"name": (*runConfigFile).Name(),
		}).Error("Encountered error while reading config file")

		return err
	}
	err = (*runConfigFile).Close()
	if err != nil {
		logger.WithError(err).WithFields(log.Fields{
			"name": (*runConfigFile).Name(),
		}).Error("Encountered error while closing config file")

		return err
	}

	workerConfig := worker.DefaultConfig.
		Clone().
		Merge(&config.Worker).
		Merge(&worker.Config{
			Logger: logger,
		})

	workr := worker.New(workerConfig)
	workr.Start(context.Background())

	go func() {
		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, unix.SIGINT, unix.SIGTERM)
		<-signalChan

		workr.SignalStop()
	}()

	err = workr.Wait()
	if err != nil {
		logger.WithError(err).WithFields(log.Fields{}).
			Error("Encountered error while running Worker")

		return err
	}

	return nil
}

func prepareLogger(level string, format string) *log.Logger {
	var handler log.Handler
	switch format {
	case "text":
		handler = text.New(os.Stdout)
	case "logfmt":
		handler = logfmt.New(os.Stdout)
	case "json":
		handler = json.New(os.Stdout)
	default:
		panic(fmt.Sprintf("unsupported log format: %s", format))
	}

	return &log.Logger{
		Level:   log.MustParseLevel(level),
		Handler: handler,
	}
}

func readConfig(file *os.File) (*YAMLConfig, error) {
	decoder := yaml.NewDecoder(file)

	yamlConfig := &YAMLConfig{}

	err := decoder.Decode(yamlConfig)
	if err != nil {
		return nil, err
	}

	return yamlConfig, nil
}
