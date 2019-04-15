package main

import (
	"bytes"
	"context"
	"errors"

	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
)

var OnDuplicateMethod = Method{
	Name:                 "onduplicate",
	SupportsBatching:     true,
	SupportsTransactions: true,
	MaxBatchSize:         6553,
	CreateRunnerFunc: func(runnerConfig *RunnerConfig) Runner {
		return &RunnerBase{
			RunnerConfig:   runnerConfig,
			ProcessingSize: runnerConfig.BatchSize,
			CreateProcessor: func(nestedRunnerConfig *RunnerConfig) ProcessBatcher {
				return &OnDuplicateMethodProcessor{
					runnerConfig: nestedRunnerConfig,
				}
			},
		}
	},
}

var _ ProcessBatcher = &OnDuplicateMethodProcessor{}

type OnDuplicateMethodProcessor struct {
	runnerConfig *RunnerConfig
	tx           *sqlx.Tx
	txCounter    int
}

func (o *OnDuplicateMethodProcessor) ProcessBatch(ctx context.Context, batch Batch) error {
	var err error
	calculatedChecksums := batch.Checksums
	fileIds := batch.Ids

	if o.tx == nil {
		err = o.openTx(ctx)
		if err != nil {
			return err
		}
	}

	files, err := o.runnerConfig.DB.FilesFetchFilesByIds(ctx, o.tx, fileIds)
	if err != nil {
		return err
	}

	insert := o.buildInsertBase()

	for ind, _ := range files {
		// Pointer to file in files, don't copy
		file := &files[ind]

		calculatedChecksum, ok := calculatedChecksums[file.ID]
		if !ok {
			// Only log warning in production
			return errors.New("Database returned unknown file")
		} else if calculatedChecksum == nil {
			// Only log warning in production
			return errors.New("Encountered duplicate file id")
		}

		if file.Checksum != nil && file.ToBeCompared == 1 &&
			!bytes.Equal(calculatedChecksum, file.Checksum) {
			o.runnerConfig.Logger.Println("Checksum mismatch discovered", file.Path)
		}

		file.Checksum = calculatedChecksums[file.ID]
		file.LastRead.Uint64, file.LastRead.Valid = o.runnerConfig.RunID, true
		file.ToBeRead = 0
		file.ToBeCompared = 0

		insert = insert.Values(
			file.ID,
			file.Rand,
			file.Path,
			file.ModificationTime,
			file.FileSize,
			file.LastSeen,
			file.ToBeRead,
			file.ToBeCompared,
			file.Checksum,
			file.LastRead,
		)

		calculatedChecksums[file.ID] = nil
	}

	// Check that all files received from the batch's input channel
	// (calculatedChecksums) have been processed
	for _, checksum := range calculatedChecksums {
		if checksum != nil {
			return errors.New("Database returned insufficient files")
		}
	}

	_, err = insert.ExecContext(ctx)
	if err != nil {
		return err
	}

	o.txCounter++
	if o.txCounter >= o.runnerConfig.TransactionSize {
		o.renewTx(ctx)
	}

	return nil
}

func (o *OnDuplicateMethodProcessor) Finalise(_ context.Context) (err error) {
	if o.tx != nil {
		err = o.closeTx()
		if err != nil {
			return
		}
	}

	return
}

const onDuplicateUpdateQuerySuffix = `
	ON DUPLICATE KEY UPDATE
		to_be_read = VALUES(to_be_read),
		to_be_compared = VALUES(to_be_compared),
		checksum = VALUES(checksum),
		last_read = VALUES(last_read)
`

func (o *OnDuplicateMethodProcessor) buildInsertBase() squirrel.InsertBuilder {
	return squirrel.Insert(o.runnerConfig.DB.FilesTableName()).
		Columns(
			"id",
			"rand",
			"path",
			"modification_time",
			"file_size",
			"last_seen",
			"to_be_read",
			"to_be_compared",
			"checksum",
			"last_read",
		).
		PlaceholderFormat(squirrel.Question).
		RunWith(o.tx).
		Suffix(onDuplicateUpdateQuerySuffix)
}

func (o *OnDuplicateMethodProcessor) openTx(ctx context.Context) (err error) {
	o.tx, err = o.runnerConfig.DB.BeginTxx(ctx, nil)
	if err != nil {
		return
	}
	o.txCounter = 0

	return
}

func (o *OnDuplicateMethodProcessor) closeTx() (err error) {
	err = o.tx.Commit()
	if err != nil {
		return
	}

	return
}

func (o *OnDuplicateMethodProcessor) renewTx(ctx context.Context) (err error) {
	err = o.closeTx()
	if err != nil {
		return
	}

	err = o.openTx(ctx)
	if err != nil {
		return
	}

	return
}
