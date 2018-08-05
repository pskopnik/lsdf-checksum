package meda

import (
	"context"

	"github.com/jmoiron/sqlx"
)

const FilesTableName = "files"

const FilesMaxPathLength = 4096

type File struct {
	Id               uint64     `db:"id"`
	Rand             float64    `db:"rand"`
	Path             string     `db:"path"`
	ModificationTime Time       `db:"modification_time"`
	FileSize         uint64     `db:"file_size"`
	LastSeen         uint64     `db:"last_seen"`
	ToBeRead         uint8      `db:"to_be_read"`
	Checksum         []byte     `db:"checksum"`
	LastRead         NullUint64 `db:"last_read"`
}

func FilesQueryCtxFilesToBeReadPaginated(ctx context.Context, querier sqlx.QueryerContext, startRand float64, startId, limit uint64) (*sqlx.Rows, error) {
	return querier.QueryxContext(
		ctx,
		"SELECT id, rand, path, file_size FROM files WHERE to_be_read = '1' AND (rand > ? OR (rand = ? AND id > ?)) ORDER BY rand, id LIMIT ?;",
		startRand,
		startRand,
		startId,
		limit,
	)
}

func FilesQueryCtxFilesByIdsForShare(ctx context.Context, querier RebindQueryerContext, fileIds []uint64) (*sqlx.Rows, error) {
	query, args, err := sqlx.In("SELECT id, path, modification_time, file_size, last_seen, checksum, last_read FROM files WHERE id IN (?) LOCK IN SHARE MODE;", fileIds)
	if err != nil {
		return nil, err
	}

	// query is a generic query using `?` as the bindvar.
	// It needs to be rebound to match the backend in use.
	query = querier.Rebind(query)

	return querier.QueryxContext(ctx, query, args...)
}

func FilesPrepareUpdateChecksum(ctx context.Context, preparer NamedPreparerContext) (*sqlx.NamedStmt, error) {
	return preparer.PrepareNamedContext(ctx, "UPDATE files SET checksum = :checksum, last_read = :last_read, to_be_read = '0' WHERE id = :id;")
}
