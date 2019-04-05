package medasync

import (
	"context"

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
	it := meda.ChunkIterator{
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
