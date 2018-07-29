package meda

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
)

const FilesTableName = "files"

const FilesMaxPathLength = 4096

type File struct {
	Id               int       `db:"id"`
	Rand             float64   `db:"rand"`
	Path             string    `db:"path"`
	ModificationTime time.Time `db:"modification_time"`
	FileSize         int       `db:"file_size"`
	LastSeen         int       `db:"last_seen"`
	ToBeRead         int       `db:"to_be_read"`
	Checksum         []byte    `db:"checksum"`
	LastRead         int       `db:"last_read"`
}

func FilesQueryCtxFilesToBeRead(ctx context.Context, querier sqlx.QueryerContext) (*sqlx.Rows, error) {
	return querier.QueryxContext(ctx, "SELECT id, path, file_size FROM files WHERE to_be_read = '1' ORDER BY rand;")
}

func FilesQueryCtxFilesByIdsForShare(ctx context.Context, querier RebindQueryerContext, fileIds []int) (*sqlx.Rows, error) {
	query, args, err := sqlx.In("SELECT id, path, modification_time, file_size, last_seen, checksum, last_read FROM files WHERE id IN (?) FOR SHARE;", fileIds)
	if err != nil {
		return nil, err
	}

	// query is a generic query using `?` as the bindvar.
	// It needs to be rebound to match the backend in use.
	query = querier.Rebind(query)

	return querier.QueryxContext(ctx, query, args...)
}

func FilesPrepareUpdateChecksum(ctx context.Context, preparer NamedPreparerContext) (*sqlx.NamedStmt, error) {
	return preparer.PrepareNamedContext(ctx, "UPDATE files SET checksum = :checksum, last_read = :last_read, to_be_read = 0 WHERE id = :id;")
}
