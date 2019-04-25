package workqueue

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"gopkg.in/tomb.v2"

	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

type transactionState int8

const (
	transactionStateUninitialised transactionState = iota
	transactionStateIdle
	transactionStateActive
	transactionStateClosed
)

var (
	txOptionsReadUncommitted = &sql.TxOptions{
		Isolation: sql.LevelReadUncommitted,
	}
)

type transactionerConfig struct {
	DB                     *meda.DB `yaml:"-"`
	MaxTransactionSize     int
	MaxTransactionLifetime time.Duration
}

type transactioner struct {
	Config *transactionerConfig
	ctx    context.Context

	updateFileCount            int
	insertChecksumWarningCount int

	// txMutex protects txState and somewhat protects tx, txQueryCount and
	// insertChecksumWarningStmt.
	// The fields tx, txQueryCount and insertChecksumWarningStmt may only be
	// read or written when holding the mutex or if txState == active only by
	// the goroutine maintaining txState.
	txMutex sync.Mutex
	txState transactionState
	tx      *sqlx.Tx
	// insertChecksumWarningStmt may be nil even if tx is set.
	insertChecksumWarningStmt *sqlx.NamedStmt
	// txQueryCount tracks the number of writing (!) queries executed within
	// the transaction.
	txQueryCount int

	tomb   *tomb.Tomb
	cancel context.CancelFunc

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

func (t *transactioner) FetchFilesByIDs(ctx context.Context, fileIDs []uint64) ([]meda.File, error) {
	t.txMutex.Lock()
	t.txState = transactionStateActive
	t.txMutex.Unlock()
	defer func() {
		t.txMutex.Lock()
		if t.txState == transactionStateActive {
			t.txState = transactionStateIdle
		}
		t.txMutex.Unlock()
	}()

	if t.tx == nil {
		err := t.beginTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "(*transactioner).FetchFilesByIDs")
		}
	}

	files, err := t.Config.DB.FilesFetchFilesByIDs(ctx, t.tx, fileIDs)
	if err != nil {
		return nil, errors.Wrap(err, "(*transactioner).FetchFilesByIDs")
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

func (t *transactioner) AppendFilesByIDs(files []meda.File, ctx context.Context, fileIDs []uint64) ([]meda.File, error) {
	t.txMutex.Lock()
	t.txState = transactionStateActive
	t.txMutex.Unlock()
	defer func() {
		t.txMutex.Lock()
		if t.txState == transactionStateActive {
			t.txState = transactionStateIdle
		}
		t.txMutex.Unlock()
	}()

	if t.tx == nil {
		err := t.beginTx(ctx)
		if err != nil {
			return files, errors.Wrap(err, "(*transactioner).FetchFilesByIDs")
		}
	}

	files, err := t.Config.DB.FilesAppendFilesByIDs(files, ctx, t.tx, fileIDs)
	if err != nil {
		return files, errors.Wrap(err, "(*transactioner).FetchFilesByIDs")
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
	t.txMutex.Lock()
	t.txState = transactionStateActive
	t.txMutex.Unlock()
	defer func() {
		t.txMutex.Lock()
		if t.txState == transactionStateActive {
			t.txState = transactionStateIdle
		}
		t.txMutex.Unlock()
	}()

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

func (t *transactioner) UpdateFilesChecksums(ctx context.Context, files []meda.File, runID uint64) error {
	if len(files) == 0 {
		return nil
	}

	t.txMutex.Lock()
	t.txState = transactionStateActive
	t.txMutex.Unlock()
	defer func() {
		t.txMutex.Lock()
		if t.txState == transactionStateActive {
			t.txState = transactionStateIdle
		}
		t.txMutex.Unlock()
	}()

	if t.tx == nil {
		err := t.beginTx(ctx)
		if err != nil {
			return errors.Wrap(err, "(*transactioner).UpdateFilesChecksums")
		}
	}

	update, fileIDs, err := t.buildUpdate(files, runID)
	if err != nil {
		return errors.Wrap(err, "(*transactioner).UpdateFilesChecksums")
	}

	_, err = update.RunWith(t.tx).ExecContext(ctx)
	t.returnInterfaceSliceToPool(fileIDs)
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

func (t *transactioner) buildUpdate(files []meda.File, runID uint64) (squirrel.UpdateBuilder, []interface{}, error) {
	checksumCaseBuilder := squirrel.Case("id")

	fileIDs := t.getInterfaceSliceFromPool()
	fileIDs = append(fileIDs, make([]interface{}, len(files))...)

	for ind, _ := range files {
		// Pointer to file in files, don't copy
		file := &files[ind]

		checksumCaseBuilder = checksumCaseBuilder.When(
			squirrel.Expr(squirrel.Placeholders(1), file.ID),
			squirrel.Expr(squirrel.Placeholders(1), file.Checksum),
		)
		fileIDs[ind] = file.ID
	}

	checksumCaseSql, checksumCaseArgs, err := checksumCaseBuilder.ToSql()
	if err != nil {
		t.returnInterfaceSliceToPool(fileIDs)
		return squirrel.UpdateBuilder{}, nil, errors.Wrap(err, "(*transactioner).buildUpdate")
	}

	update := squirrel.Update(t.Config.DB.FilesTableName()).
		Set("to_be_read", 0).
		Set("to_be_compared", 0).
		Set("checksum", squirrel.Expr(checksumCaseSql, checksumCaseArgs...)).
		Set("last_read", runID).
		Where("id IN ("+squirrel.Placeholders(len(fileIDs))+")", fileIDs...)

	return update, fileIDs, nil
}

func (t *transactioner) transactionCommitter() error {
	timer := time.NewTimer(t.Config.MaxTransactionLifetime)

	select {
	case <-timer.C:
		break
	case <-t.tomb.Dying():
		return tomb.ErrDying
	}

	tx, insertChecksumWarningStmt, err := t.stealTransaction(t.tomb.Context(nil))
	if err != nil {
		// TODO
		// tx and insertChecksumWarningStmt are now potentially idling because ctx was closed
		return errors.Wrap(err, "(*transactioner).transactionCommitter")
	} else if tx == nil {
		// No open transaction stolen, there is no need for closing
		return nil
	}

	var retErr error

	if insertChecksumWarningStmt != nil {
		err = insertChecksumWarningStmt.Close()
		if err != nil && retErr == nil {
			retErr = errors.Wrap(err, "(*transactioner).transactionCommitter: close insert checksum warning statement")
		}
	}

	err = tx.Commit()
	if err != nil && retErr == nil {
		retErr = errors.Wrap(err, "(*transactioner).transactionCommitter: commit transaction")
	}

	return retErr
}

func (t *transactioner) stealTransaction(ctx context.Context) (*sqlx.Tx, *sqlx.NamedStmt, error) {
	var tx *sqlx.Tx
	var insertChecksumWarningStmt *sqlx.NamedStmt
	var noTxn bool

	timer := time.NewTimer(0)
	done := ctx.Done()

	for {
		t.txMutex.Lock()

		if t.txState == transactionStateIdle {
			tx, insertChecksumWarningStmt = t.tx, t.insertChecksumWarningStmt
			t.tx, t.txState, t.txQueryCount = nil, transactionStateUninitialised, 0
			t.insertChecksumWarningStmt = nil
		} else if t.txState == transactionStateActive {
			// wait
		} else {
			// tx does not represent an open transaction
			noTxn = true
		}

		t.txMutex.Unlock()

		if tx != nil {
			break
		} else if noTxn {
			break
		}

		timer.Reset(500 * time.Microsecond)

		select {
		case <-timer.C:
			continue
		case <-done:
			// Exhaust timer
			if !timer.Stop() {
				<-timer.C
			}

			return tx, insertChecksumWarningStmt, ctx.Err()
		}
	}

	return tx, insertChecksumWarningStmt, nil
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
	var tx *sqlx.Tx
	var insertChecksumWarningStmt *sqlx.NamedStmt

	t.cancel()
	// No context, so cannot wait for caller signalled cancel event
	<-t.tomb.Dead()

	t.txMutex.Lock()
	if t.txState == transactionStateIdle || t.txState == transactionStateActive {
		tx, insertChecksumWarningStmt = t.tx, t.insertChecksumWarningStmt
		t.tx, t.txState, t.txQueryCount = nil, transactionStateUninitialised, 0
		t.insertChecksumWarningStmt = nil
	}
	t.txMutex.Unlock()

	if insertChecksumWarningStmt != nil {
		err := insertChecksumWarningStmt.Close()
		if err != nil && retErr == nil {
			retErr = errors.Wrap(err, "(*transactioner).Close: close insert checksum warning statement")
		}
	}

	if tx != nil {
		err := tx.Rollback()
		if err != nil && retErr == nil {
			retErr = errors.Wrap(err, "(*transactioner).Close: rollback transaction")
		}
	}

	return retErr

}

// beginTx begins a new transaction and sets t.tx.
// t.txQueryCount is resetted as well.
// txState == active must be held by the caller of this method.
func (t *transactioner) beginTx(_ context.Context) error {
	tx, err := t.Config.DB.BeginTxx(t.ctx, txOptionsReadUncommitted)
	if err != nil {
		return errors.Wrap(err, "(*transactioner).beginTx: begin transaction")
	}

	t.tx, t.txQueryCount = tx, 0

	tombCtx, cancel := context.WithCancel(t.ctx)
	t.cancel = cancel
	t.tomb, _ = tomb.WithContext(tombCtx)
	t.tomb.Go(t.transactionCommitter)

	return nil
}

// prepInsertChecksumWarningStmt prepares and sets t.insertChecksumWarningStmt.
// txState == active must be held by the caller of this method.
func (t *transactioner) prepInsertChecksumWarningStmt(ctx context.Context) error {
	insertChecksumWarningStmt, err := t.Config.DB.ChecksumWarningsPrepareInsert(ctx, t.tx)
	if err != nil {
		return errors.Wrap(err, "(*transactioner).prepInsertChecksumWarningStmt")
	}

	t.insertChecksumWarningStmt = insertChecksumWarningStmt

	return nil
}

func (t *transactioner) commitTx() error {
	var retErr error
	var tx *sqlx.Tx
	var insertChecksumWarningStmt *sqlx.NamedStmt

	t.cancel()
	// No context, so cannot wait for caller signalled cancel event
	<-t.tomb.Dead()

	t.txMutex.Lock()
	if t.txState == transactionStateIdle || t.txState == transactionStateActive {
		tx, insertChecksumWarningStmt = t.tx, t.insertChecksumWarningStmt
		t.tx, t.txState, t.txQueryCount = nil, transactionStateUninitialised, 0
		t.insertChecksumWarningStmt = nil
	}
	t.txMutex.Unlock()

	if insertChecksumWarningStmt != nil {
		err := t.insertChecksumWarningStmt.Close()
		if err != nil && retErr == nil {
			retErr = errors.Wrap(err, "(*transactioner).commitTx: close insert checksum warning statement")
		}
	}

	if tx != nil {
		err := tx.Commit()
		if err != nil {
			// TODO check for conflicts, re-perform transaction
			if retErr == nil {
				retErr = errors.Wrap(err, "(*transactioner).commitTx: commit transaction")
			}
		}
	}

	return retErr
}
