package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"

	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
)

var DeleteInsertMethod = Method{
	Name:                 "deleteinsert",
	SupportsBatching:     true,
	SupportsTransactions: true,
	MaxBatchSize:         6553,
	CreateRunnerFunc: func(runnerConfig *RunnerConfig) Runner {
		return &RunnerBase{
			RunnerConfig:   runnerConfig,
			ProcessingSize: runnerConfig.BatchSize,
			CreateProcessor: func(nestedRunnerConfig *RunnerConfig) ProcessBatcher {
				return &DeleteInsertMethodProcessor{
					runnerConfig: nestedRunnerConfig,
				}
			},
		}
	},
}

var _ ProcessBatcher = &DeleteInsertMethodProcessor{}

type DeleteInsertMethodProcessor struct {
	runnerConfig *RunnerConfig
	tx           *sqlx.Tx
	txCounter    int
}

func (d *DeleteInsertMethodProcessor) ProcessBatch(ctx context.Context, batch Batch) error {
	var err error
	calculatedChecksums := batch.Checksums
	fileIds := batch.Ids

	if d.tx == nil {
		err = d.openTx(ctx)
		if err != nil {
			return err
		}
	}

	files, err := d.runnerConfig.DB.FilesFetchFilesByIds(ctx, d.tx, fileIds)
	if err != nil {
		return err
	}

	insert := d.buildInsertBase()

	for ind, _ := range files {
		// Pointer to file in files, don't copy
		file := &files[ind]

		calculatedChecksum, ok := calculatedChecksums[file.Id]
		if !ok {
			// Only log warning in production
			return errors.New("Database returned unknown file")
		} else if calculatedChecksum == nil {
			// Only log warning in production
			return errors.New("Encountered duplicate file id")
		}

		if file.Checksum != nil && file.ToBeCompared == 1 &&
			!bytes.Equal(calculatedChecksum, file.Checksum) {
			d.runnerConfig.Logger.Println("Checksum mismatch discovered", file.Path)
		}

		file.Checksum = calculatedChecksums[file.Id]
		file.LastRead.Uint64, file.LastRead.Valid = d.runnerConfig.RunId, true
		file.ToBeRead = 0
		file.ToBeCompared = 0

		insert = insert.Values(
			file.Id,
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

		calculatedChecksums[file.Id] = nil
	}

	// Check that all files received from the batch's input channel
	// (calculatedChecksums) have been processed
	for _, checksum := range calculatedChecksums {
		if checksum != nil {
			return errors.New("Database returned insufficient files")
		}
	}

	_, err = d.filesDeleteFilesByIds(ctx, d.tx, fileIds)
	if err != nil {
		return err
	}

	_, err = insert.ExecContext(ctx)
	if err != nil {
		return err
	}

	d.txCounter += 2
	if d.txCounter >= d.runnerConfig.TransactionSize {
		d.renewTx(ctx)
	}

	return nil
}

func (d *DeleteInsertMethodProcessor) Finalise(_ context.Context) (err error) {
	if d.tx != nil {
		err = d.closeTx()
		if err != nil {
			return
		}
	}

	return
}

func (d *DeleteInsertMethodProcessor) buildInsertBase() squirrel.InsertBuilder {
	return squirrel.Insert(d.runnerConfig.DB.FilesTableName()).
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
		RunWith(d.tx)
}

const filesDeleteFilesByIdsQuery = GenericQuery(`
	DELETE FROM {FILES}
		WHERE id IN (?)
	;
`)

func (d *DeleteInsertMethodProcessor) filesDeleteFilesByIds(ctx context.Context, querier RebindExecerContext, fileIds []uint64) (sql.Result, error) {
	if querier == nil {
		querier = d.runnerConfig.DB
	}

	query, args, err := sqlx.In(filesDeleteFilesByIdsQuery.SubstituteAll(d.runnerConfig.DB), fileIds)
	if err != nil {
		return nil, err
	}

	// query is a generic query using `?` as the bindvar.
	// It needs to be rebound to match the backend in use.
	query = querier.Rebind(query)

	return querier.ExecContext(ctx, query, args...)
}

func (d *DeleteInsertMethodProcessor) openTx(ctx context.Context) (err error) {
	d.tx, err = d.runnerConfig.DB.BeginTxx(ctx, nil)
	if err != nil {
		return
	}
	d.txCounter = 0

	return
}

func (d *DeleteInsertMethodProcessor) closeTx() (err error) {
	err = d.tx.Commit()
	if err != nil {
		return
	}

	return
}

func (d *DeleteInsertMethodProcessor) renewTx(ctx context.Context) (err error) {
	err = d.closeTx()
	if err != nil {
		return
	}

	err = d.openTx(ctx)
	if err != nil {
		return
	}

	return
}
