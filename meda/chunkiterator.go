package meda

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// ChunkIterator is a utility to iterate over fixed-size chunks of a table.
// Each chunk consists of a number of rows whose Ids are in between two
// values. ChunkIterator has no associated resources and does not have to be
// closed.
type ChunkIterator struct {
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
func (c *ChunkIterator) Next(ctx context.Context, queryer sqlx.QueryerContext) bool {
	if c.err != nil || c.finished {
		return false
	}

	var lastId uint64

	err := queryer.QueryRowxContext(ctx, c.NextChunkQuery, c.lastId, c.ChunkSize).Scan(&lastId)
	if err == sql.ErrNoRows {
		c.finished = true
		return false
	} else if err != nil {
		c.err = errors.Wrap(err, "(*ChunkIterator).Next: query next chunk")
		return false
	}

	c.previousId, c.lastId = c.lastId, lastId

	return true
}

// Err returns any error which occurred during querying the next chunk and
// interpreting the result.
func (c *ChunkIterator) Err() error {
	return c.err
}

// PreviousId returns the last id of the previous chunk. This method may only
// be called after Next has returned true.
func (c *ChunkIterator) PreviousId() uint64 {
	return c.previousId
}

// LastId returns the last id of the current chunk. This method may only be
// called after Next has returned true.
func (c *ChunkIterator) LastId() uint64 {
	return c.lastId
}
