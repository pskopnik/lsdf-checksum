package meda

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
)

const InsertsTableName = "inserts"

type Insert struct {
	Id               int       `db:"id"`
	Rand             float64   `db:"rand"`
	Path             string    `db:"path"`
	ModificationTime time.Time `db:"modification_time"`
	FileSize         int       `db:"file_size"`
	LastSeen         int       `db:"last_seen"`
}

func InsertsPrepareInsert(ctx context.Context, preparer NamedPreparerContext) (*sqlx.NamedStmt, error) {
	return preparer.PrepareNamedContext(ctx, "INSERT INTO inserts (rand, path, modification_time, file_size, last_seen) VALUES (RAND(), :path, :modification_time, :file_size, :last_seen);")
}
