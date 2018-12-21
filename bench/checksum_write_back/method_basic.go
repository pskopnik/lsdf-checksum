package main

import (
	"bytes"
	"context"
	"errors"

	"github.com/jmoiron/sqlx"
)

var BasicMethod = Method{
	Name:                 "basic",
	SupportsBatching:     false,
	SupportsTransactions: true,
	CreateRunnerFunc: func(runnerConfig *RunnerConfig) Runner {
		return &RunnerBase{
			RunnerConfig:   runnerConfig,
			ProcessingSize: runnerConfig.TransactionSize,
			CreateProcessor: func(nestedRunnerConfig *RunnerConfig) ProcessBatcher {
				return &BasicMethodProcessor{
					runnerConfig: nestedRunnerConfig,
				}
			},
		}
	},
}

var _ ProcessBatcher = &BasicMethodProcessor{}

type BasicMethodProcessor struct {
	runnerConfig *RunnerConfig
}

func (b *BasicMethodProcessor) ProcessBatch(ctx context.Context, batch Batch) error {
	calculatedChecksums := batch.Checksums
	fileIds := batch.Ids

	tx, filesUpdatePrepStmt, err := b.openTxAndStmts(ctx)
	if err != nil {
		return err
	}

	files, err := b.runnerConfig.DB.FilesFetchFilesByIds(ctx, tx, fileIds)
	if err != nil {
		_ = b.closeTxAndStmts(tx, filesUpdatePrepStmt)
		return err
	}

	for ind, _ := range files {
		// Pointer to file in files, don't copy
		file := &files[ind]

		calculatedChecksum, ok := calculatedChecksums[file.Id]
		if !ok {
			_ = b.closeTxAndStmts(tx, filesUpdatePrepStmt)
			// Only log warning in production
			return errors.New("Database returned unknown file")
		} else if calculatedChecksum == nil {
			_ = b.closeTxAndStmts(tx, filesUpdatePrepStmt)
			// Only log warning in production
			return errors.New("Encountered duplicate file id")
		}

		if file.Checksum != nil && file.ToBeCompared == 1 &&
			!bytes.Equal(calculatedChecksum, file.Checksum) {
			b.runnerConfig.Logger.Println("Checksum mismatch discovered", file.Path)
		}

		file.Checksum = calculatedChecksums[file.Id]
		file.LastRead.Uint64, file.LastRead.Valid = b.runnerConfig.RunId, true
		file.ToBeRead = 0
		file.ToBeCompared = 0

		_, err = filesUpdatePrepStmt.ExecContext(ctx, file)
		if err != nil {
			_ = b.closeTxAndStmts(tx, filesUpdatePrepStmt)
			// Only log warning in production?
			return err
		}

		calculatedChecksums[file.Id] = nil
	}

	// Check that all files received from the batch's input channel
	// (calculatedChecksums) have been processed
	for _, checksum := range calculatedChecksums {
		if checksum != nil {
			_ = b.closeTxAndStmts(tx, filesUpdatePrepStmt)
			return errors.New("Database returned insufficient files")
		}
	}

	err = b.closeTxAndStmts(tx, filesUpdatePrepStmt)
	if err != nil {
		return err
	}

	return nil
}

func (_ *BasicMethodProcessor) Finalise(_ context.Context) error {
	return nil
}

func (b *BasicMethodProcessor) openTxAndStmts(ctx context.Context) (
	tx *sqlx.Tx,
	filesUpdatePrepStmt *sqlx.NamedStmt,
	err error,
) {
	tx, err = b.runnerConfig.DB.BeginTxx(ctx, nil)
	if err != nil {
		return
	}

	filesUpdatePrepStmt, err = b.filesPrepareUpdateChecksum(ctx, tx)
	if err != nil {
		// Close everything hitherto
		_ = tx.Commit()
		return
	}

	return
}

func (b *BasicMethodProcessor) closeTxAndStmts(
	tx *sqlx.Tx,
	filesUpdatePrepStmt *sqlx.NamedStmt,
) (err error) {
	err = filesUpdatePrepStmt.Close()
	if err != nil {
		// Attempt cleanup
		_ = tx.Commit()
		return
	}

	err = tx.Commit()
	if err != nil {
		return
	}

	return
}

const filesPrepareUpdateChecksumQuery = GenericQuery(`
	UPDATE {FILES}
		SET
			checksum = :checksum,
			last_read = :last_read,
			to_be_read = '0',
			to_be_compared = '0'
		WHERE id = :id
	;
`)

func (b *BasicMethodProcessor) filesPrepareUpdateChecksum(ctx context.Context, preparer NamedPreparerContext) (*sqlx.NamedStmt, error) {
	if preparer == nil {
		preparer = b.runnerConfig.DB
	}

	return preparer.PrepareNamedContext(ctx, filesPrepareUpdateChecksumQuery.SubstituteAll(b.runnerConfig.DB))
}
