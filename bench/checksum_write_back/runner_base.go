package main

import (
	"context"
	"time"

	"gopkg.in/tomb.v2"
)

type ProcessBatcher interface {
	ProcessBatch(context.Context, Batch) error
	// Finalise finalises the processing of any remaining records and cleans
	// up resources held by the ProcessBatcher.
	//
	// Finalise must be called on every ProcessBatcher in case ProcessBatch()
	// has been called at least once. Finalise may be called with a cancelled
	// context or after ProcessBatch() has returned an error.
	Finalise(context.Context) error
}

type RunnerBase struct {
	RunnerConfig    *RunnerConfig
	ProcessingSize  int
	CreateProcessor func(*RunnerConfig) ProcessBatcher
	Prepare         func(ctx context.Context) error
	Finalise        func(ctx context.Context) error
	TearDown        func(ctx context.Context) error
}

func (r *RunnerBase) Run(ctx context.Context) (RunResult, error) {
	if r.Prepare != nil {
		err := r.Prepare(ctx)
		if err != nil {
			return RunResult{}, err
		}
	}

	batchProcessor := r.CreateProcessor(r.RunnerConfig)

	startTime := time.Now()

	done := ctx.Done()

	for {
		select {
		case <-done:
			_ = batchProcessor.Finalise(ctx)
			if r.Finalise != nil {
				_ = r.Finalise(ctx)
			}
			if r.TearDown != nil {
				_ = r.TearDown(ctx)
			}
			return RunResult{}, ctx.Err()
		default:
		}

		batch := r.RunnerConfig.Generator.CollectBatch(r.ProcessingSize)

		err := batchProcessor.ProcessBatch(ctx, batch)
		if err != nil {
			_ = batchProcessor.Finalise(ctx)
			if r.Finalise != nil {
				_ = r.Finalise(ctx)
			}
			if r.TearDown != nil {
				_ = r.TearDown(ctx)
			}
			return RunResult{}, err
		}

		if r.RunnerConfig.Generator.Done() {
			break
		}
	}

	err := batchProcessor.Finalise(ctx)
	if err != nil {
		if r.Finalise != nil {
			_ = r.Finalise(ctx)
		}
		if r.TearDown != nil {
			_ = r.TearDown(ctx)
		}
		return RunResult{}, err
	}

	if r.Finalise != nil {
		err = r.Finalise(ctx)
		if err != nil {
			if r.TearDown != nil {
				_ = r.TearDown(ctx)
			}
			return RunResult{}, err
		}
	}

	endTime := time.Now()

	if r.TearDown != nil {
		err = r.TearDown(ctx)
		if err != nil {
			return RunResult{}, err
		}
	}

	return RunResult{
		TotalDuration: endTime.Sub(startTime),
	}, nil
}

func (r *RunnerBase) RunConcurrently(ctx context.Context) (RunResult, error) {
	workerTomb, _ := tomb.WithContext(ctx)
	batchChan := make(chan Batch, 1)

	if r.Prepare != nil {
		err := r.Prepare(ctx)
		if err != nil {
			return RunResult{}, err
		}
	}

	workerTomb.Go(func() error {
		workerTomb.Go(func() error {
			dying := workerTomb.Dying()
			for {
				batch := r.RunnerConfig.Generator.CollectBatch(r.ProcessingSize)

				select {
				case batchChan <- batch:
				case <-dying:
					close(batchChan)
					return nil
				}

				if r.RunnerConfig.Generator.Done() {
					break
				}
			}
			close(batchChan)
			return nil
		})

		for i := 0; i < r.RunnerConfig.Concurrency; i++ {
			workerTomb.Go(func() error {
				batchProcessor := r.CreateProcessor(r.RunnerConfig)
				ctx := workerTomb.Context(context.Background())

				for batch := range batchChan {
					err := batchProcessor.ProcessBatch(ctx, batch)
					if err != nil {
						_ = batchProcessor.Finalise(ctx)
						return err
					}
				}

				err := batchProcessor.Finalise(ctx)
				if err != nil {
					return err
				}

				return nil
			})
		}

		return nil
	})

	startTime := time.Now()

	workerTomb.Wait()

	if err := workerTomb.Err(); err != nil {
		if r.Finalise != nil {
			_ = r.Finalise(ctx)
		}
		if r.TearDown != nil {
			_ = r.TearDown(ctx)
		}
		return RunResult{}, err
	}

	if r.Finalise != nil {
		err := r.Finalise(ctx)
		if err != nil {
			return RunResult{}, err
		}
	}

	endTime := time.Now()

	if r.TearDown != nil {
		err := r.TearDown(ctx)
		if err != nil {
			return RunResult{}, err
		}
	}

	return RunResult{
		TotalDuration: endTime.Sub(startTime),
	}, nil
}
