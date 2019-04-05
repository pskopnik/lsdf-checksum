package workqueue

import (
	"context"
	"sync"

	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"

	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

type transactionerConfig struct {
	DB                 *meda.DB `yaml:"-"`
	MaxTransactionSize int
}

type transactioner struct {
	Config *transactionerConfig
	ctx    context.Context

	updateFileCount            int
	insertChecksumWarningCount int

	tx                        *sqlx.Tx
	insertChecksumWarningStmt *sqlx.NamedStmt
	// txQueryCount tracks the number of writing (!) queries executed within the transaction.
	txQueryCount int

	interfaceSlicesPool sync.Pool
}

// newTransactioner creates and returns an transactioner from ctx and config.
// ctx is used as the transaction context for all transactions begun by the
// transactioner.
func newTransactioner(ctx context.Context, config *transactionerConfig) *transactioner {
	return &transactioner{
		Config: config,
		ctx:    ctx,
	}
}

func (t *transactioner) FetchFilesByIds(ctx context.Context, fileIds []uint64) ([]meda.File, error) {
	if t.tx == nil {
		err := t.beginTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "(*transactioner).FetchFilesByIds")
		}
	}

	files, err := t.Config.DB.FilesFetchFilesByIds(ctx, t.tx, fileIds)
	if err != nil {
		return nil, errors.Wrap(err, "(*transactioner).FetchFilesByIds")
	}

	// t.txQueryCount += 1

	// if t.txQueryCount >= t.Config.MaxTransactionSize {
	// 	err := t.commitTx()
	// 	if err != nil {
	// 		return errors.Wrap(err, "(*transactioner).InsertChecksumWarning")
	// 	}
	// }

	return files, nil
}

func (t *transactioner) AppendFilesByIds(files []meda.File, ctx context.Context, fileIds []uint64) ([]meda.File, error) {
	if t.tx == nil {
		err := t.beginTx(ctx)
		if err != nil {
			return files, errors.Wrap(err, "(*transactioner).FetchFilesByIds")
		}
	}

	files, err := t.Config.DB.FilesAppendFilesByIds(files, ctx, t.tx, fileIds)
	if err != nil {
		return files, errors.Wrap(err, "(*transactioner).FetchFilesByIds")
	}

	// t.txQueryCount += 1

	// if t.txQueryCount >= t.Config.MaxTransactionSize {
	// 	err := t.commitTx()
	// 	if err != nil {
	// 		return errors.Wrap(err, "(*transactioner).InsertChecksumWarning")
	// 	}
	// }

	return files, nil
}

func (t *transactioner) InsertChecksumWarning(ctx context.Context, checksumWarning *meda.ChecksumWarning) error {
	if t.tx == nil {
		err := t.beginTx(ctx)
		if err != nil {
			return errors.Wrap(err, "(*transactioner).InsertChecksumWarning")
		}
	}

	if t.insertChecksumWarningStmt == nil {
		err := t.prepInsertChecksumWarningStmt(ctx)
		if err != nil {
			return errors.Wrap(err, "(*transactioner).InsertChecksumWarning")
		}
	}

	_, err := t.insertChecksumWarningStmt.ExecContext(ctx, &checksumWarning)
	if err != nil {
		return errors.Wrap(err, "(*transactioner).InsertChecksumWarning")
	}

	t.insertChecksumWarningCount += 1
	t.txQueryCount += 1

	if t.txQueryCount >= t.Config.MaxTransactionSize {
		err := t.commitTx()
		if err != nil {
			return errors.Wrap(err, "(*transactioner).InsertChecksumWarning")
		}
	}

	return nil
}

func (t *transactioner) UpdateFilesChecksums(ctx context.Context, files []meda.File, runId uint64) error {
	if t.tx == nil {
		err := t.beginTx(ctx)
		if err != nil {
			return errors.Wrap(err, "(*transactioner).UpdateFilesChecksums")
		}
	}

	update, fileIds, err := t.buildUpdate(files, runId)
	if err != nil {
		return errors.Wrap(err, "(*transactioner).UpdateFilesChecksums")
	}

	_, err = update.RunWith(t.tx).ExecContext(ctx)
	t.returnInterfaceSliceToPool(fileIds)
	if err != nil {
		return errors.Wrap(err, "(*transactioner).UpdateFilesChecksums: exec query")
	}

	t.updateFileCount += 1
	t.txQueryCount += 1

	if t.txQueryCount >= t.Config.MaxTransactionSize {
		err := t.commitTx()
		if err != nil {
			return errors.Wrap(err, "(*transactioner).UpdateFilesChecksums")
		}
	}

	return nil
}

func (t *transactioner) buildUpdate(files []meda.File, runId uint64) (squirrel.UpdateBuilder, []interface{}, error) {
	checksumCaseBuilder := squirrel.Case("id")

	fileIds := t.getInterfaceSliceFromPool()
	fileIds = append(fileIds, make([]interface{}, len(files))...)

	for ind, _ := range files {
		// Pointer to file in files, don't copy
		file := &files[ind]

		checksumCaseBuilder = checksumCaseBuilder.When(
			squirrel.Expr(squirrel.Placeholders(1), file.Id),
			squirrel.Expr(squirrel.Placeholders(1), file.Checksum),
		)
		fileIds[ind] = file.Id
	}

	checksumCaseSql, checksumCaseArgs, err := checksumCaseBuilder.ToSql()
	if err != nil {
		t.returnInterfaceSliceToPool(fileIds)
		return squirrel.UpdateBuilder{}, nil, errors.Wrap(err, "(*transactioner).buildUpdate")
	}

	update := squirrel.Update(t.Config.DB.FilesTableName()).
		Set("to_be_read", 0).
		Set("to_be_compared", 0).
		Set("checksum", squirrel.Expr(checksumCaseSql, checksumCaseArgs...)).
		Set("last_read", runId).
		Where("id IN ("+squirrel.Placeholders(len(fileIds))+")", fileIds...)

	return update, fileIds, nil
}

func (t *transactioner) getInterfaceSliceFromPool() []interface{} {
	sl := t.interfaceSlicesPool.Get()
	if sl == nil {
		return nil
	}

	return sl.([]interface{})
}

func (t *transactioner) returnInterfaceSliceToPool(sl []interface{}) {
	if cap(sl) == 0 {
		return
	}

	sl = sl[:0]
	t.interfaceSlicesPool.Put(sl)
}

func (t *transactioner) UpdateFileCount() int {
	return t.updateFileCount
}

func (t *transactioner) InsertChecksumWarningCount() int {
	return t.insertChecksumWarningCount
}

func (t *transactioner) Commit() error {
	return t.commitTx()
}

func (t *transactioner) Close() error {
	var retErr error

	if t.insertChecksumWarningStmt != nil {
		err := t.insertChecksumWarningStmt.Close()
		t.insertChecksumWarningStmt = nil
		if err != nil && retErr == nil {
			retErr = errors.Wrap(err, "(*transactioner).Close: close insert checksum warning statement")
		}
	}

	if t.tx != nil {
		err := t.tx.Rollback()
		t.tx, t.txQueryCount = nil, 0
		if err != nil && retErr == nil {
			retErr = errors.Wrap(err, "(*transactioner).Close: rollback transaction")
		}
	}

	return retErr

}

func (t *transactioner) beginTx(_ context.Context) error {
	var err error

	t.tx, err = t.Config.DB.BeginTxx(t.ctx, nil)
	t.txQueryCount = 0
	if err != nil {
		t.tx = nil
		return errors.Wrap(err, "(*transactioner).beginTx: begin transaction")
	}

	return nil
}

func (t *transactioner) prepInsertChecksumWarningStmt(ctx context.Context) error {
	var err error

	t.insertChecksumWarningStmt, err = t.Config.DB.ChecksumWarningsPrepareInsert(ctx, t.tx)
	if err != nil {
		t.tx = nil
		return errors.Wrap(err, "(*transactioner).prepInsertChecksumWarningStmt")
	}

	return nil
}

func (t *transactioner) commitTx() error {
	var retErr error

	if t.insertChecksumWarningStmt != nil {
		err := t.insertChecksumWarningStmt.Close()
		t.insertChecksumWarningStmt = nil
		if err != nil && retErr == nil {
			retErr = errors.Wrap(err, "(*transactioner).commitTx: close insert checksum warning statement")
		}
	}

	if t.tx != nil {
		err := t.tx.Commit()
		t.tx, t.txQueryCount = nil, 0
		if err != nil {
			// TODO check for conflicts, re-perform transaction
			if retErr == nil {
				retErr = errors.Wrap(err, "(*transactioner).commitTx: commit transaction")
			}
		}
	}

	return retErr
}
