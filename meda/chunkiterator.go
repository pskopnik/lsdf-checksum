package meda

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// ChunkIterator is a utility to iterate over fixed-size chunks of a table.
// Each chunk consists of a number of rows whose IDs are in between two
// values. ChunkIterator has no associated resources and does not have to be
// closed.
type ChunkIterator struct {
	// NextChunkQuery is the query used to retrieve the last id of the next
	// chunk. The query is passed two parameters: 1) The last id of the
	// previous chunk and 2) the chunk size.
	NextChunkQuery string
	ChunkSize      uint64

	finished   bool
	err        error
	previousID uint64
	lastID     uint64
}

// Next queries the next chunk of size ChunkSize using NextChunkQuery. If
// false is returned either the end of the table has been reached (no more
// chunks) or an error occurred. The user must check Err to distinguish
// between the two cases.
//
// The ids required to run queries on the chunk can be retrieved through
// PreviousID and LastID. These methods may only be used after Next has
// returned true. Each chunk consists of all records with id > PreviousID and
// id <= LastID.
func (c *ChunkIterator) Next(ctx context.Context, queryer sqlx.QueryerContext) bool {
	if c.err != nil || c.finished {
		return false
	}

	var lastID uint64

	err := queryer.QueryRowxContext(ctx, c.NextChunkQuery, c.lastID, c.ChunkSize).Scan(&lastID)
	if err == sql.ErrNoRows {
		c.finished = true
		return false
	} else if err != nil {
		c.err = errors.Wrap(err, "(*ChunkIterator).Next: query next chunk")
		return false
	}

	c.previousID, c.lastID = c.lastID, lastID

	return true
}

// Err returns any error which occurred during querying the next chunk and
// interpreting the result.
func (c *ChunkIterator) Err() error {
	return c.err
}

// PreviousID returns the last id of the previous chunk. This method may only
// be called after Next has returned true.
func (c *ChunkIterator) PreviousID() uint64 {
	return c.previousID
}

// LastID returns the last id of the current chunk. This method may only be
// called after Next has returned true.
func (c *ChunkIterator) LastID() uint64 {
	return c.lastID
}

// ChunkIteratorByRand is a utility to iterate over approximately fixed-size
// chunks of a table using a column containing RAND() values.
// ChunkIteratorByRand is a modified version of ChunkIterator working with
// float64 instead of uint64 values.
// Each chunk consists of a number of rows whose rand values are in between
// two values. ChunkIteratorByRand has no associated resources and does not
// have to be closed.
type ChunkIteratorByRand struct {
	// NextChunkQuery is the query used to retrieve the last rand value of the
	// next chunk. The query is passed two parameters: 1) The last rand value
	// of the previous chunk and 2) the chunk size.
	NextChunkQuery string
	ChunkSize      uint64

	finished     bool
	err          error
	previousRand float64
	lastRand     float64
}

// Next queries the next chunk of size ChunkSize using NextChunkQuery. If
// false is returned either the end of the table has been reached (no more
// chunks) or an error occurred. The user must check Err to distinguish
// between the two cases.
//
// The rand values required to run queries on the chunk can be retrieved
// through PreviousRand and LastRand. These methods may only be used after
// Next has returned true. Each chunk consists of all records with rand >
// PreviousRand and id <= LastRand.
// This may be more records than specified by ChunkSize, as rand is not a
// unique column. However, each chunk still refers to distinct set of rows.
// No row is contained in two chunks.
func (c *ChunkIteratorByRand) Next(ctx context.Context, queryer sqlx.QueryerContext) bool {
	if c.err != nil || c.finished {
		return false
	}

	var lastRand float64

	err := queryer.QueryRowxContext(ctx, c.NextChunkQuery, c.lastRand, c.ChunkSize).Scan(&lastRand)
	if err == sql.ErrNoRows {
		c.finished = true
		return false
	} else if err != nil {
		c.err = errors.Wrap(err, "(*ChunkIteratorByRand).Next: query next chunk")
		return false
	}

	c.previousRand, c.lastRand = c.lastRand, lastRand

	return true
}

// Err returns any error which occurred during querying the next chunk and
// interpreting the result.
func (c *ChunkIteratorByRand) Err() error {
	return c.err
}

// PreviousRand returns the last rand value of the previous chunk. This method
// may only be called after Next has returned true.
func (c *ChunkIteratorByRand) PreviousRand() float64 {
	return c.previousRand
}

// LastRand returns the last rand value of the current chunk. This method may
// only be called after Next has returned true.
func (c *ChunkIteratorByRand) LastRand() float64 {
	return c.lastRand
}
