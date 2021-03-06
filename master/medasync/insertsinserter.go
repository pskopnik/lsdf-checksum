package medasync

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"

	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

type insertsInserterConfig struct {
	DB                 *meda.DB `yaml:"-"`
	MaxTransactionSize int
}

type insertsInserter struct {
	Config *insertsInserterConfig
	ctx    context.Context

	insertsCount int

	tx             *sqlx.Tx
	insertPrepStmt *sqlx.NamedStmt
	txQueryCount   int
}

// newInsertsInserter creates and returns an insertsTransaction from ctx
// and config.
// ctx is used as the transaction context for all transactions begun by the
// transactioner.
func newInsertsInserter(ctx context.Context, config *insertsInserterConfig) *insertsInserter {
	return &insertsInserter{
		Config: config,
		ctx:    ctx,
	}
}

func (i *insertsInserter) Insert(ctx context.Context, insert *meda.Insert) error {
	if i.tx == nil {
		err := i.beginTx(ctx)
		if err != nil {
			return errors.Wrap(err, "(*insertsInserter).Insert")
		}
	}

	_, err := i.insertPrepStmt.ExecContext(ctx, insert)
	if err != nil {
		return errors.Wrap(err, "(*insertsInserter).Insert: exec write insert statement")
	}

	i.insertsCount += 1
	i.txQueryCount += 1

	if i.txQueryCount >= i.Config.MaxTransactionSize {
		err := i.commitTx()
		if err != nil {
			return errors.Wrap(err, "(*insertsInserter).Insert")
		}
	}

	return nil
}

func (i *insertsInserter) InsertsCount() int {
	return i.insertsCount
}

func (i *insertsInserter) Commit() error {
	return i.commitTx()
}

func (i *insertsInserter) Close() error {
	var retErr error

	if i.insertPrepStmt != nil {
		err := i.insertPrepStmt.Close()
		i.insertPrepStmt = nil
		if err != nil && retErr == nil {
			retErr = errors.Wrap(err, "(*insertsInserter).Close: close insert statement")
		}
	}

	if i.tx != nil {
		err := i.tx.Rollback()
		i.tx, i.txQueryCount = nil, 0
		if err != nil && retErr == nil {
			retErr = errors.Wrap(err, "(*insertsInserter).Close: rollback transaction")
		}
	}

	return retErr

}

// beginTx begins a new transaction and prepares the insert statement. i.ctx
// is used as the context of the transaction created. ctx is used as the
// context for any statements executed.
// beginTx does not close a still open transaction. Implied precondition:
// i.tx and i.insertPrepStmt == nil.
// If no error is returned, i.tx and i.insertPrepStmt are valid and
// txQueryCount is 0.
// If an error is returned, i.tx and i.insertPrepStmt == nil.
func (i *insertsInserter) beginTx(ctx context.Context) error {
	var err error

	i.tx, err = i.Config.DB.BeginTxx(i.ctx, nil)
	i.txQueryCount = 0
	if err != nil {
		i.tx = nil
		return errors.Wrap(err, "(*insertsInserter).beginTx: begin transaction")
	}

	i.insertPrepStmt, err = i.Config.DB.InsertsPrepareInsert(ctx, i.tx)
	if err != nil {
		_ = i.tx.Rollback()
		i.tx, i.insertPrepStmt = nil, nil
		return errors.Wrap(err, "(*insertsInserter).beginTx: prepare inserts statement")
	}

	return nil
}

// commitTx commits the open transaction and closes all associated resources.
// commitTx attempts to commit the transaction and close resources regardless
// of whether an error is returned.
// commitTx always sets i.tx and i.insertPrepStmt == nil.
func (i *insertsInserter) commitTx() error {
	var retErr error

	if i.insertPrepStmt != nil {
		err := i.insertPrepStmt.Close()
		i.insertPrepStmt = nil
		if err != nil && retErr == nil {
			retErr = errors.Wrap(err, "(*insertsInserter).commitTx: close prepared statement")
		}
	}

	if i.tx != nil {
		err := i.tx.Commit()
		i.tx, i.txQueryCount = nil, 0
		if err != nil {
			// TODO check for conflicts, re-perform transaction
			if retErr == nil {
				retErr = errors.Wrap(err, "(*insertsInserter).commitTx: commit transaction")
			}
		}
	}

	return retErr
}
