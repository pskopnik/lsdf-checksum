package main

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"

	"gopkg.in/yaml.v2"

	_ "github.com/go-sql-driver/mysql"
)

type YAMLCrossConfig struct {
	Methods []string
	Values  BenchmarkCrossConfig
}

type YAMLConfig struct {
	DB          DBConfig
	Benchmarker BenchmarkerConfig
	Cross       YAMLCrossConfig
}

func main() {
	logger := log.New(os.Stderr, "", 0)

	config, err := readConfig(os.Args[1])
	if err != nil {
		logger.Println("Encountered error while reading config file", err)
		os.Exit(1)
	}

	dbConfig := DBDefaultConfig.
		Clone().
		Merge(&config.DB).
		Merge(&DBConfig{})

	db, err := Open(dbConfig)
	if err != nil {
		logger.Println("Database open returned error", err)
		os.Exit(1)
	}
	defer db.Close()

	benchmarkerConfig := BenchmarkerDefaultConfig.
		Clone().
		Merge(&config.Benchmarker).
		Merge(&BenchmarkerConfig{
			DB:     db,
			Logger: logger,
		})

	benchmarker := NewBenchmarker(benchmarkerConfig)

	methods, err := collectMethods(config.Cross.Methods)
	if err != nil {
		logger.Println("Unable to collect methods", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, os.Interrupt, os.Kill)
		<-signalChan

		cancel()
	}()

	results, err := benchmarker.PerformCross(ctx, config.Cross.Values, methods)
	if err != nil {
		logger.Println("Encountered error during benchmarking", err)
		os.Exit(1)
	}

	writer := csv.NewWriter(os.Stdout)
	err = writeHeader(writer)
	if err != nil {
		logger.Println("Encountered error while writing CSV results header", err)
		os.Exit(1)
	}
	for _, result := range results {
		err = writeRow(writer, &result)
		if err != nil {
			logger.Println("Encountered error while writing CSV results row", err)
			os.Exit(1)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		logger.Println("Encountered error while flushing CSV writer", err)
		os.Exit(1)
	}
}

func collectMethods(methodNames []string) ([]*Method, error) {
	collectedMethods := make([]*Method, len(methodNames))
L:
	for i, methodName := range methodNames {
		for _, method := range Methods {
			if method.Name == methodName {
				collectedMethods[i] = method
				continue L
			}
		}
		return []*Method{}, errors.New(fmt.Sprintln("method not found, method name", methodName))
	}

	return collectedMethods, nil
}

func writeHeader(writer *csv.Writer) error {
	return writer.Write([]string{
		"method",
		"batch_size",
		"transaction_size",
		"concurrent",
		"concurrency",
		"total_duration",
	})
}

func writeRow(writer *csv.Writer, result *BenchmarkResult) error {
	return writer.Write([]string{
		result.Method.Name,
		strconv.FormatInt(int64(result.RunVariation.BatchSize), 10),
		strconv.FormatInt(int64(result.RunVariation.TransactionSize), 10),
		strconv.FormatBool(result.RunVariation.Concurrent),
		strconv.FormatInt(int64(result.RunVariation.Concurrency), 10),
		strconv.FormatInt(result.RunResult.TotalDuration.Nanoseconds(), 10),
	})
}

func readConfig(path string) (*YAMLConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := yaml.NewDecoder(file)

	yamlConfig := &YAMLConfig{}

	err = decoder.Decode(yamlConfig)
	if err != nil {
		return nil, err
	}

	return yamlConfig, nil
}
