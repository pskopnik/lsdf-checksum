package meda

import (
	"context"

	"github.com/jmoiron/sqlx"
)

const filesTableNameBase = "files"

func (d *DB) FilesTableName() string {
	return d.Config.TablePrefix + filesTableNameBase
}

const FilesMaxPathLength = 4096

// https://mariadb.com/kb/en/library/building-the-best-index-for-a-given-select/
// https://dev.mysql.com/doc/refman/5.7/en/create-index.html
// https://mariadb.com/kb/en/library/xtradbinnodb-server-system-variables/#innodb_large_prefix
// https://dev.mysql.com/doc/refman/5.7/en/innodb-parameters.html#sysvar_innodb_large_prefix

const filesCreateTableQuery = GenericQuery(`
	CREATE TABLE IF NOT EXISTS {FILES} (
		id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
		rand double NOT NULL,
		path varbinary(4096) NOT NULL,
		modification_time datetime(6) NOT NULL,
		file_size bigint(20) unsigned NOT NULL,
		last_seen bigint(20) unsigned NOT NULL,
		to_be_read tinyint(3) unsigned NOT NULL DEFAULT 1,
		to_be_compared tinyint(3) unsigned NOT NULL DEFAULT 0,
		checksum varbinary(64) DEFAULT NULL,
		last_read bigint(20) unsigned DEFAULT NULL,
		PRIMARY KEY (id),
		KEY rand (rand),
		KEY path (path(2048))
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`)

func (d *DB) filesCreateTable(ctx context.Context) error {
	_, err := d.ExecContext(ctx, filesCreateTableQuery.SubstituteAll(d))
	return err
}

type File struct {
	Id               uint64     `db:"id"`
	Rand             float64    `db:"rand"`
	Path             string     `db:"path"`
	ModificationTime Time       `db:"modification_time"`
	FileSize         uint64     `db:"file_size"`
	LastSeen         uint64     `db:"last_seen"`
	ToBeRead         uint8      `db:"to_be_read"`
	ToBeCompared     uint8      `db:"to_be_compared"`
	Checksum         []byte     `db:"checksum"`
	LastRead         NullUint64 `db:"last_read"`
}

const filesQueryFilesToBeReadPaginatedQuery = GenericQuery(`
	SELECT
		id, rand, path, file_size
	FROM {FILES}
		WHERE
				to_be_read = '1'
			AND
				(rand > ? OR (rand = ? AND id > ?))
		ORDER BY rand, id ASC
		LIMIT ?
	;
`)

func (d *DB) FilesQueryFilesToBeReadPaginated(ctx context.Context, querier sqlx.QueryerContext, startRand float64, startId, limit uint64) (*sqlx.Rows, error) {
	if querier == nil {
		querier = &d.DB
	}

	return querier.QueryxContext(
		ctx,
		filesQueryFilesToBeReadPaginatedQuery.SubstituteAll(d),
		startRand,
		startRand,
		startId,
		limit,
	)
}

const filesQueryFilesByIdsForShareQuery = GenericQuery(`
	SELECT
		id, path, modification_time, file_size, last_seen, to_be_compared, checksum, last_read
	FROM {FILES}
		WHERE id IN (?)
		LOCK IN SHARE MODE
	;
`)

func (d *DB) FilesQueryFilesByIdsForShare(ctx context.Context, querier RebindQueryerContext, fileIds []uint64) (*sqlx.Rows, error) {
	if querier == nil {
		querier = &d.DB
	}

	query, args, err := sqlx.In(filesQueryFilesByIdsForShareQuery.SubstituteAll(d), fileIds)
	if err != nil {
		return nil, err
	}

	// query is a generic query using `?` as the bindvar.
	// It needs to be rebound to match the backend in use.
	query = querier.Rebind(query)

	return querier.QueryxContext(ctx, query, args...)
}

const filesPrepareUpdateChecksumQuery = GenericQuery(`
	UPDATE {FILES}
		SET
			checksum = :checksum,
			last_read = :last_read,
			to_be_read = '0',
			to_be_compared = '0'
		WHERE id = :id
	;
`)

func (d *DB) FilesPrepareUpdateChecksum(ctx context.Context, preparer NamedPreparerContext) (*sqlx.NamedStmt, error) {
	if preparer == nil {
		preparer = &d.DB
	}

	return preparer.PrepareNamedContext(ctx, filesPrepareUpdateChecksumQuery.SubstituteAll(d))
}
