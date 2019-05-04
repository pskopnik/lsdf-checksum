package main

import (
	"fmt"
	"os"

	"github.com/apex/log"
	"github.com/apex/log/handlers/discard"
	"github.com/apex/log/handlers/json"
	"github.com/apex/log/handlers/logfmt"
	"github.com/apex/log/handlers/text"
)

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
		panic(fmt.Sprintf("unsupported log format: '%s'", format))
	}

	return &log.Logger{
		Level:   log.MustParseLevel(level),
		Handler: handler,
	}
}

func prepareNoOpLogger() *log.Logger {
	return &log.Logger{
		Level:   log.FatalLevel,
		Handler: discard.New(),
	}
}
