package main

import (
	"bytes"
	"context"
	"errors"

	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
)

var UpdateCaseMethod = Method{
	Name:                 "updatecase",
	SupportsBatching:     true,
	SupportsTransactions: true,
	MaxBatchSize:         21844,
	CreateRunnerFunc: func(runnerConfig *RunnerConfig) Runner {
		return &RunnerBase{
			RunnerConfig:   runnerConfig,
			ProcessingSize: runnerConfig.BatchSize,
			CreateProcessor: func(nestedRunnerConfig *RunnerConfig) ProcessBatcher {
				return &UpdateCaseMethodProcessor{
					runnerConfig: nestedRunnerConfig,
				}
			},
		}
	},
}

var _ ProcessBatcher = &UpdateCaseMethodProcessor{}

type UpdateCaseMethodProcessor struct {
	runnerConfig *RunnerConfig
	tx           *sqlx.Tx
	txCounter    int
}

func (u *UpdateCaseMethodProcessor) ProcessBatch(ctx context.Context, batch Batch) error {
	var err error
	calculatedChecksums := batch.Checksums
	fileIds := batch.Ids

	if u.tx == nil {
		err = u.openTx(ctx)
		if err != nil {
			return err
		}
	}

	files, err := u.runnerConfig.DB.FilesFetchFilesByIds(ctx, u.tx, fileIds)
	if err != nil {
		return err
	}

	checksumCaseBuilder := squirrel.Case("id")
	fileIdInterfaces := make([]interface{}, len(files))

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
			u.runnerConfig.Logger.Println("Checksum mismatch discovered", file.Path)
		}

		file.Checksum = calculatedChecksums[file.Id]
		file.LastRead.Uint64, file.LastRead.Valid = u.runnerConfig.RunId, true
		file.ToBeRead = 0
		file.ToBeCompared = 0

		checksumCaseBuilder = checksumCaseBuilder.When(
			squirrel.Expr(squirrel.Placeholders(1), file.Id),
			squirrel.Expr(squirrel.Placeholders(1), file.Checksum),
		)
		fileIdInterfaces[ind] = file.Id

		calculatedChecksums[file.Id] = nil
	}

	// Check that all files received from the batch's input channel
	// (calculatedChecksums) have been processed
	for _, checksum := range calculatedChecksums {
		if checksum != nil {
			return errors.New("Database returned insufficient files")
		}
	}

	update, err := u.buildUpdate(checksumCaseBuilder, fileIdInterfaces)
	if err != nil {
		return err
	}

	_, err = update.ExecContext(ctx)
	if err != nil {
		return err
	}

	u.txCounter++
	if u.txCounter >= u.runnerConfig.TransactionSize {
		u.renewTx(ctx)
	}

	return nil
}

func (u *UpdateCaseMethodProcessor) Finalise(_ context.Context) (err error) {
	if u.tx != nil {
		err = u.closeTx()
		if err != nil {
			return
		}
	}

	return
}

func (u *UpdateCaseMethodProcessor) buildUpdate(
	checksumCaseBuilder squirrel.CaseBuilder,
	fileIds []interface{},
) (squirrel.UpdateBuilder, error) {
	checksumCaseSql, checksumCaseArgs, err := checksumCaseBuilder.ToSql()
	if err != nil {
		return squirrel.UpdateBuilder{}, err
	}

	update := squirrel.Update(u.runnerConfig.DB.FilesTableName()).
		Set("to_be_read", 0).
		Set("to_be_compared", 0).
		Set("checksum", squirrel.Expr(checksumCaseSql, checksumCaseArgs...)).
		Set("last_read", u.runnerConfig.RunId).
		Where("id IN ("+squirrel.Placeholders(len(fileIds))+")", fileIds...).
		RunWith(u.tx)

	return update, nil
}

func (u *UpdateCaseMethodProcessor) openTx(ctx context.Context) (err error) {
	u.tx, err = u.runnerConfig.DB.BeginTxx(ctx, nil)
	if err != nil {
		return
	}
	u.txCounter = 0

	return
}

func (u *UpdateCaseMethodProcessor) closeTx() (err error) {
	err = u.tx.Commit()
	if err != nil {
		return
	}

	return
}

func (u *UpdateCaseMethodProcessor) renewTx(ctx context.Context) (err error) {
	err = u.closeTx()
	if err != nil {
		return
	}

	err = u.openTx(ctx)
	if err != nil {
		return
	}

	return
}
