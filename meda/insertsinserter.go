package meda

import (
	"context"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"git.scc.kit.edu/sdm/lsdf-checksum/internal/batcher"
)

//go:generate confions config Config

type InsertsInserterConfig struct {
	// RowsPerStmt is the (max) number of rows in a single INSERT statement.
	RowsPerStmt int
	// StmtsPerTransaction is the (max) number of INSERT statements per
	// transaction.
	StmtsPerTransaction int
	// Concurrency is the number of concurrently running workers, executing
	// queries on the database.
	Concurrency int
	// MaxWaitTime is the maximum duration a row waits before being included
	// in a batch and queued for insertion.
	// A batch is the set of rows inserted in a single transaction.
	MaxWaitTime time.Duration
	// BatchQueueSize is the number of batches which may be queued for
	// inserting.
	// Recommended values are within [1, Concurrency].
	BatchQueueSize int
}

var InsertsInserterDefaultConfig = &InsertsInserterConfig{
	RowsPerStmt:         2000,
	StmtsPerTransaction: 1,
	Concurrency:         1,
	MaxWaitTime:         time.Minute,
	BatchQueueSize:      1,
}

type InsertsInserterStats struct {
	InsertsAdded          int
	InsertsCommitted      int
	StatementsCommitted   int
	TransactionsCommitted int
}

// InsertsInserter offers fast row insertions into the inserts table.
type InsertsInserter struct {
	config InsertsInserterConfig
	db     *DB

	ctx   context.Context
	group *errgroup.Group

	batcher *batcher.Batcher[*Insert]

	insertsAdded          atomic.Int64
	insertsCommitted      atomic.Int64
	statementsCommitted   atomic.Int64
	transactionsCommitted atomic.Int64
}

// Add adds a row to be inserted with the data in insert.
// The return of Add does not mean the row has been inserted, it may still
// reside in memory or be uncommitted.
// The ctx only affects the Add method call.
func (i *InsertsInserter) Add(ctx context.Context, insert *Insert) error {
	err := i.batcher.Add(ctx, insert)
	if err != nil {
		return err
	}

	i.insertsAdded.Add(1)

	return nil
}

// Stats returns statistics about the current state of the inserter.
// The different metrics within the snapshot may not be consistent.
// Stats returns valid information also after the inserter has been closed.
func (i *InsertsInserter) Stats() InsertsInserterStats {
	return InsertsInserterStats{
		InsertsAdded:          int(i.insertsAdded.Load()),
		InsertsCommitted:      int(i.insertsCommitted.Load()),
		StatementsCommitted:   int(i.statementsCommitted.Load()),
		TransactionsCommitted: int(i.transactionsCommitted.Load()),
	}
}

// Close wraps up all open work and then closes all used resources.
//
// Close must always be called to ensure Add()ed rows are actually inserted.
func (i *InsertsInserter) Close(ctx context.Context) error {
	var err, rErr error

	err = i.batcher.Close(ctx)
	if err != nil {
		rErr = err
	}

	err = i.group.Wait()
	if err != nil && rErr == nil {
		rErr = err
	}

	return rErr
}

func (i *InsertsInserter) insertWorker() error {
	var j int
	// If i.ctx is cancelled, i.batcher.Out() is closed as well.
	for batch := range i.batcher.Out() {
		err := i.insertBatch(i.ctx, batch)
		if err != nil {
			return err
		}
		j++
	}

	return nil
}

const insertsInsertBatchQuery = GenericQuery(`
	INSERT INTO {INSERTS} (
			path, modification_time, file_size
		) VALUES (
			:path, :modification_time, :file_size
		)
	;
`)

func (i *InsertsInserter) insertBatch(ctx context.Context, batch []*Insert) error {
	var totalRows int64
	var statements int64

	tx, err := i.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for len(batch) > 0 {
		stmtBatch := batch
		if len(batch) > i.config.RowsPerStmt {
			stmtBatch = stmtBatch[:i.config.RowsPerStmt]
		}

		res, err := tx.NamedExecContext(
			ctx,
			insertsInsertBatchQuery.SubstituteAll(i.db),
			stmtBatch,
		)
		if err != nil {
			return err
		}
		ra, err := res.RowsAffected()
		if err != nil {
			return err
		}
		totalRows += ra
		statements++

		batch = batch[len(stmtBatch):]
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	i.insertsCommitted.Add(totalRows)
	i.statementsCommitted.Add(statements)
	i.transactionsCommitted.Add(1)

	return nil
}

// NewInsertsInserter returns an InsertsInserter for fast row insertion.
//
// ctx is used for the entire lifetime of the inserter, i.e. cancelling ctx
// rolls back currently open transactions and makes the inserter unusable.
func (d *DB) NewInsertsInserter(ctx context.Context, config InsertsInserterConfig) *InsertsInserter {
	group, groupCtx := errgroup.WithContext(ctx)
	i := &InsertsInserter{
		config: config,
		group:  group,
		ctx:    groupCtx,
		db:     d,
		batcher: batcher.New[*Insert](groupCtx, &batcher.Config{
			MaxItems:    config.RowsPerStmt * config.StmtsPerTransaction,
			MaxWaitTime: config.MaxWaitTime,
		}),
	}

	for j := 0; j < config.Concurrency; j++ {
		group.Go(i.insertWorker)
	}

	return i
}
