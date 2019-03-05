package medasync

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"

	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

type chunker struct {
	ChunkSize      uint64
	NextChunkQuery string

	DB *meda.DB

	BeginTx      func(ctx context.Context, db *meda.DB) (*sqlx.Tx, error)
	ProcessChunk func(ctx context.Context, db *meda.DB, tx *sqlx.Tx, prevId, lastId uint64) error
}

func (c *chunker) Run(ctx context.Context) error {
	it := chunkIterator{
		NextChunkQuery: c.NextChunkQuery,
		ChunkSize:      c.ChunkSize,
	}

	tx, err := c.beginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "(*chunker).Run: begin transaction")
	}

	for it.Next(ctx, tx) {
		err = c.ProcessChunk(ctx, c.DB, tx, it.PreviousId(), it.LastId())
		if err != nil {
			_ = tx.Rollback()
			return errors.Wrap(err, "(*chunker).Run: ProcessChunk execution")
		}

		err = tx.Commit()
		if err != nil {
			return errors.Wrap(err, "(*chunker).Run: commit transaction")
		}

		tx, err = c.beginTx(ctx)
		if err != nil {
			return errors.Wrap(err, "(*chunker).Run: begin transaction")
		}
	}
	if err := it.Err(); err != nil {
		_ = tx.Rollback()
		return errors.Wrap(err, "(*chunker).Run: chunk reader")
	}

	if err := tx.Rollback(); err != nil {
		return errors.Wrap(err, "(*chunker).Run: rollback empty transaction")
	}

	return nil
}

func (c *chunker) beginTx(ctx context.Context) (*sqlx.Tx, error) {
	if c.BeginTx == nil {
		return c.DB.BeginTxx(ctx, nil)
	}

	return c.BeginTx(ctx, c.DB)
}

// chunkIterator is a utility to iterate over fixed-size chunks of a table.
// Each chunk consists of a number of rows whose Ids are in between two
// values. chunkIterator has no associated resources and does not have to be
// closed.
type chunkIterator struct {
	// NextChunkQuery is the query used to retrieve the last id of the next
	// chunk. The query is passed two parameters: 1) The last id of the previous
	// chunk and 2) the chunk size.
	NextChunkQuery string
	ChunkSize      uint64

	finished   bool
	err        error
	previousId uint64
	lastId     uint64
}

// Next queries the next chunk of size ChunkSize using NextChunkQuery. If
// false is returned either the end of the table has been reached (no more
// chunks) or an error occurred. The user must check Err to distinguish
// between the two cases.
//
// The ids required to run queries on the chunk can be retrieved through
// PreviousId and LastId. These methods may only be used after Next has
// returned true. Each chunk consists of all records with id > PreviousId and
// id <= LastId.
func (c *chunkIterator) Next(ctx context.Context, tx *sqlx.Tx) bool {
	if c.err != nil || c.finished {
		return false
	}

	var lastId uint64

	err := tx.QueryRowContext(ctx, c.NextChunkQuery, c.lastId, c.ChunkSize).Scan(&lastId)
	if err == sql.ErrNoRows {
		c.finished = true
		return false
	} else if err != nil {
		c.err = errors.Wrap(err, "(*chunkIterator).Next: query next chunk")
		return false
	}

	c.previousId, c.lastId = c.lastId, lastId

	return true
}

// Err returns any error which occurred during querying the next chunk and
// interpreting the result.
func (c *chunkIterator) Err() error {
	return c.err
}

// PreviousId returns the last id of the previous chunk. This method may only
// be called after Next has returned true.
func (c *chunkIterator) PreviousId() uint64 {
	return c.previousId
}

// LastId returns the last id of the current chunk. This method may only be
// called after Next has returned true.
func (c *chunkIterator) LastId() uint64 {
	return c.lastId
}
