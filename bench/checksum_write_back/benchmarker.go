package main

import (
	"context"
	cryptoRand "crypto/rand"
	"log"
	"math"
	"math/big"
	"math/rand"
	"time"
)

type RunVariation struct {
	BatchSize       int
	TransactionSize int
	Concurrent      bool
	Concurrency     int
}

type RunnerConfig struct {
	RunVariation
	RunID     uint64
	Logger    *log.Logger
	DB        *DB
	Generator *Generator
}

type RunResult struct {
	TotalDuration time.Duration
}

type Runner interface {
	// Run performs a run with the passed in configuration and returns a
	// RunResult struct containing relevant timings.
	//
	// Run must never be called concurrently on the same Runner instance.
	Run(context.Context) (RunResult, error)
	// RunConcurrently performs a run with the passed in configuration and
	// returns a RunResult struct containing relevant timings. RunConcurrently
	// distributes the work onto a number of concurrent processors. The level
	// of concurrency is configured via the Runner's configuration.
	//
	// RunConcurrently must never be called concurrently on the same Runner
	// instance.
	RunConcurrently(context.Context) (RunResult, error)
}

type Method struct {
	Name                 string
	SupportsBatching     bool
	SupportsTransactions bool
	MaxBatchSize         int
	CreateRunnerFunc     func(*RunnerConfig) Runner
}

type BenchmarkResult struct {
	Method *Method
	RunVariation
	RunResult
}

type BenchmarkCrossConfig struct {
	// BatchSize is a slice of values for the BatchSize option to be
	// benchmarked.
	BatchSize []int
	// TransactionSize is a slice of values for the TransactionSize option to
	// be benchmarked.
	TransactionSize []int
	// Concurrency is a slice of values for the Concurrency option to be
	// benchmarked. The value 0 has special meaning: Concurrent running is
	// disabled and the Concurrency option is set to 1.
	Concurrency []int
}

//go:generate confions config BenchmarkerConfig

type BenchmarkerConfig struct {
	DB                        *DB         `yaml:"-"`
	Logger                    *log.Logger `yaml:"-"`
	GenerateAndWriteVariation RunVariation
	ChecksumLength            int
}

var BenchmarkerDefaultConfig = &BenchmarkerConfig{
	GenerateAndWriteVariation: RunVariation{
		BatchSize:       1000,
		TransactionSize: 10,
		Concurrent:      true,
		Concurrency:     20,
	},
	ChecksumLength: 20,
}

type Benchmarker struct {
	config *BenchmarkerConfig
	src    rand.Source
	seed   int64
}

func NewBenchmarker(config *BenchmarkerConfig) *Benchmarker {
	return &Benchmarker{
		config: config,
	}
}

func (b *Benchmarker) Prepare() error {
	seed, err := b.generateSeed()
	if err != nil {
		return err
	}

	b.seed = seed

	return nil
}

func (b *Benchmarker) Perform(
	ctx context.Context,
	variation RunVariation,
	method *Method,
) (BenchmarkResult, error) {
	b.src = rand.NewSource(b.seed)

	err := b.prepareFilesTable(ctx, method)
	if err != nil {
		return BenchmarkResult{}, err
	}

	minID, maxID, err := b.getFilesMinMaxID(ctx)
	if err != nil {
		return BenchmarkResult{}, err
	}

	generator := newGenerator(minID, maxID, b.config.ChecksumLength, b.src)
	generator.prepare()

	runnerConfig := &RunnerConfig{
		RunVariation: variation,
		RunID:        2,
		Logger:       b.config.Logger,
		DB:           b.config.DB,
		Generator:    generator,
	}

	runner := method.CreateRunnerFunc(runnerConfig)

	var runResult RunResult
	if variation.Concurrent {
		runResult, err = runner.RunConcurrently(ctx)
	} else {
		runResult, err = runner.Run(ctx)
	}
	if err != nil {
		runnerConfig.Logger.Printf("%+v %s\n", variation, method.Name)
		return BenchmarkResult{}, err
	}

	err = b.dropFilesTable(ctx)
	if err != nil {
		return BenchmarkResult{}, err
	}

	return BenchmarkResult{
		Method:       method,
		RunVariation: variation,
		RunResult:    runResult,
	}, nil
}

func (b *Benchmarker) PerformCross(
	ctx context.Context,
	crossValues BenchmarkCrossConfig,
	methods []*Method,
) ([]BenchmarkResult, error) {
	var results []BenchmarkResult

	for iBatchSize, batchSize := range crossValues.BatchSize {
		for iTransactionSize, transactionSize := range crossValues.TransactionSize {
			for _, concurrency := range crossValues.Concurrency {
				for _, method := range methods {
					variation := RunVariation{
						BatchSize:       batchSize,
						TransactionSize: transactionSize,
						Concurrent:      true,
						Concurrency:     concurrency,
					}

					if method.SupportsBatching {
					} else if iBatchSize == 0 {
						variation.BatchSize = 1
					} else {
						continue
					}
					if method.SupportsTransactions {
					} else if iTransactionSize == 0 {
						variation.TransactionSize = 1
					} else {
						continue
					}

					if method.SupportsBatching && variation.BatchSize > method.MaxBatchSize {
						continue
					}

					if concurrency == 0 {
						variation.Concurrent = false
						variation.Concurrency = 1
					}

					result, err := b.Perform(ctx, variation, method)
					if err != nil {
						return []BenchmarkResult{}, err
					}
					results = append(results, result)
				}
			}
		}
	}

	return results, nil
}

func (b *Benchmarker) generateSeed() (int64, error) {
	// max is (1 << 63) - 1 + 1, resulting in the range [0, 1 << 63 - 1] for cryptoRand.Int
	max := big.NewInt(0).SetInt64(math.MaxInt64)
	max.Add(max, big.NewInt(1))
	bigInt, err := cryptoRand.Int(cryptoRand.Reader, max)
	if err != nil {
		return 0, err
	}

	return bigInt.Int64(), nil
}

const dropFilesQuery = GenericQuery(`
	DROP TABLE IF EXISTS {FILES};
`)

const populateFilesQuery = GenericQuery(`
	INSERT INTO {FILES}
		(rand, path, modification_time, file_size, last_seen, to_be_read, to_be_compared)
			SELECT
				RAND(), path, modification_time, file_size, 1, 1, 0
			FROM {FILE_DATA}
	;
`)

const advanceRunsOfAllFilesQuery = GenericQuery(`
	UPDATE {FILES}
		SET
			modification_time = ADDTIME(files.modification_time, 600),
			last_seen = 2,
			to_be_read = 1,
			to_be_compared = 0
	;
`)

func (b *Benchmarker) prepareFilesTable(ctx context.Context, method *Method) error {
	err := b.dropFilesTable(ctx)
	if err != nil {
		return err
	}

	err = b.config.DB.filesCreateTable(ctx)
	if err != nil {
		return err
	}

	_, err = b.config.DB.ExecContext(ctx, populateFilesQuery.SubstituteAll(b.config.DB))
	if err != nil {
		return err
	}

	err = b.generateAndWriteFilesChecksums(ctx, method)
	if err != nil {
		return err
	}

	_, err = b.config.DB.ExecContext(ctx, advanceRunsOfAllFilesQuery.SubstituteAll(b.config.DB))
	if err != nil {
		return err
	}

	return nil
}

func (b *Benchmarker) generateAndWriteFilesChecksums(ctx context.Context, method *Method) error {
	minID, maxID, err := b.getFilesMinMaxID(ctx)
	if err != nil {
		return err
	}

	generator := newGenerator(minID, maxID, b.config.ChecksumLength, b.src)
	generator.prepare()

	runnerConfig := &RunnerConfig{
		RunVariation: b.config.GenerateAndWriteVariation,
		Logger:       b.config.Logger,
		DB:           b.config.DB,
		Generator:    generator,
	}

	runner := method.CreateRunnerFunc(runnerConfig)

	if b.config.GenerateAndWriteVariation.Concurrent {
		_, err = runner.RunConcurrently(ctx)
	} else {
		_, err = runner.Run(ctx)
	}
	if err != nil {
		return err
	}

	return nil
}

func (b *Benchmarker) dropFilesTable(ctx context.Context) error {
	_, err := b.config.DB.ExecContext(ctx, dropFilesQuery.SubstituteAll(b.config.DB))
	if err != nil {
		return err
	}

	return nil
}

const getFilesMinMaxIDQuery = GenericQuery(`
	SELECT
		min(id) as min_id, max(id) as max_id
	FROM {FILES}
	;
`)

func (b *Benchmarker) getFilesMinMaxID(ctx context.Context) (uint64, uint64, error) {
	row := b.config.DB.QueryRowxContext(ctx, getFilesMinMaxIDQuery.SubstituteAll(b.config.DB))

	var minID uint64
	var maxID uint64

	err := row.Scan(&minID, &maxID)
	if err != nil {
		return 0, 0, err
	}

	return minID, maxID, nil
}
