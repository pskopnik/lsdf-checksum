package main

import (
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	app = kingpin.New("lsdf-checksum-master", "Master command of the lsdf-checksum system.")
)

func main() {
	var err error

	switch kingpin.MustParse(app.Parse(os.Args[1:])) {
	case run.FullCommand():
		err = performRun()
	case initialiseRun.FullCommand():
		err = performInitialiseRun()
	case complete.FullCommand():
		err = performComplete()
	case abort.FullCommand():
		err = performAbort()
	case runs.FullCommand():
		err = performRuns()
	case warnings.FullCommand():
		err = performWarnings()
	}

	if err != nil {
		if mainErr, ok := err.(*MainError); ok {
			os.Exit(mainErr.ExitCode)
			return
		}
		os.Exit(1)
	}
}

var _ error = &MainError{}

type MainError struct {
	error
	ExitCode int
}

func (m *MainError) Cause() error {
	return m.error
}
