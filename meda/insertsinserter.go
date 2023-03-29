package meda

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type InsertsInserterConfig struct {
	MaxTransactionSize int
}

type InsertsInserterStats struct {
	InsertsCount int
}

// InsertsInserter offers fast row insertions into the inserts table.
type InsertsInserter struct {
	Config *InsertsInserterConfig
	ctx    context.Context
	db     *DB

	insertsCount int

	tx             *sqlx.Tx
	insertPrepStmt *sqlx.NamedStmt
	txQueryCount   int
}

// Add adds a row to be inserted with the data in insert.
// The return of Add does not mean the row has been inserted, it may still
// reside in memory or be uncommitted.
// The ctx only affects the Add method call.
func (i *InsertsInserter) Add(ctx context.Context, insert *Insert) error {
	if i.tx == nil {
		err := i.beginTx(i.ctx)
		if err != nil {
			return errors.Wrap(err, "(*insertsInserter).Add")
		}
	}

	_, err := i.insertPrepStmt.ExecContext(ctx, insert)
	if err != nil {
		return errors.Wrap(err, "(*insertsInserter).Add: exec write insert statement")
	}

	i.insertsCount += 1
	i.txQueryCount += 1

	if i.txQueryCount >= i.Config.MaxTransactionSize {
		err := i.commitTx()
		if err != nil {
			return errors.Wrap(err, "(*insertsInserter).Add")
		}
	}

	return nil
}

// Stats returns statistics about the current state of the inserter.
func (i *InsertsInserter) Stats() InsertsInserterStats {
	return InsertsInserterStats{
		InsertsCount: i.insertsCount,
	}
}

// Close wraps up all open work and then closes all used resources.
//
// Close must always be called to ensure Add()ed rows are actually inserted.
func (i *InsertsInserter) Close() error {
	return i.commitTx()
}

// beginTx begins a new transaction and prepares the insert statement. i.ctx
// is used as the context of the transaction created. ctx is used as the
// context for any statements executed.
// beginTx does not close a still open transaction. Implied precondition:
// i.tx and i.insertPrepStmt == nil.
// If no error is returned, i.tx and i.insertPrepStmt are valid and
// txQueryCount is 0.
// If an error is returned, i.tx and i.insertPrepStmt == nil.
func (i *InsertsInserter) beginTx(ctx context.Context) error {
	var err error

	i.tx, err = i.db.BeginTxx(i.ctx, nil)
	i.txQueryCount = 0
	if err != nil {
		i.tx = nil
		return errors.Wrap(err, "(*insertsInserter).beginTx: begin transaction")
	}

	i.insertPrepStmt, err = i.db.insertsPrepareInsert(ctx, i.tx)
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
func (i *InsertsInserter) commitTx() error {
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

// NewInsertsInserter returns an InsertsInserter for fast row insertion.
//
// ctx is used for the entire lifetime of the inserter, i.e. cancelling ctx
// makes the inserter unusable and rolls back currently open transactions.
func (d *DB) NewInsertsInserter(ctx context.Context, config *InsertsInserterConfig) *InsertsInserter {
	return &InsertsInserter{
		Config: config,
		ctx: ctx,
		db: d,
	}
}
