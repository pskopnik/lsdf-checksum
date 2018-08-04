package meda

import (
	"context"

	"github.com/jmoiron/sqlx"
)

const InsertsTableName = "inserts"

type Insert struct {
	Id               uint64  `db:"id"`
	Rand             float64 `db:"rand"`
	Path             string  `db:"path"`
	ModificationTime Time    `db:"modification_time"`
	FileSize         uint64  `db:"file_size"`
	LastSeen         uint64  `db:"last_seen"`
}

func InsertsPrepareInsert(ctx context.Context, preparer NamedPreparerContext) (*sqlx.NamedStmt, error) {
	return preparer.PrepareNamedContext(ctx, "INSERT INTO inserts (rand, path, modification_time, file_size, last_seen) VALUES (RAND(), :path, :modification_time, :file_size, :last_seen);")
}
