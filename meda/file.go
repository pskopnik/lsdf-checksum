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
	ID               uint64     `db:"id"`
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

func filesAppendFromRowsAndClose(files []File, rows *sqlx.Rows) ([]File, error) {
	baseInd := len(files)

	var err error
	i := baseInd

	for rows.Next() {
		if i == cap(files) {
			files = append(files, File{})
		} else {
			files = files[:len(files)+1]
		}

		err = rows.StructScan(&files[i])
		if err != nil {
			_ = rows.Close()
			return files[:baseInd], err
		}

		i += 1
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return files[:baseInd], err
	}

	if err = rows.Close(); err != nil {
		return files[:baseInd], err
	}

	return files, nil
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

func (d *DB) FilesQueryFilesToBeReadPaginated(ctx context.Context, querier sqlx.QueryerContext, startRand float64, startID, limit uint64) (*sqlx.Rows, error) {
	if querier == nil {
		querier = &d.DB
	}

	return querier.QueryxContext(
		ctx,
		filesQueryFilesToBeReadPaginatedQuery.SubstituteAll(d),
		startRand,
		startRand,
		startID,
		limit,
	)
}

func (d *DB) FilesFetchFilesToBeReadPaginated(ctx context.Context, querier sqlx.QueryerContext, startRand float64, startID, limit uint64) ([]File, error) {
	return d.FilesAppendFilesToBeReadPaginated(nil, ctx, querier, startRand, startID, limit)
}

func (d *DB) FilesAppendFilesToBeReadPaginated(files []File, ctx context.Context, querier sqlx.QueryerContext, startRand float64, startID, limit uint64) ([]File, error) {
	rows, err := d.FilesQueryFilesToBeReadPaginated(ctx, querier, startRand, startID, limit)
	if err != nil {
		return files, err
	}

	return filesAppendFromRowsAndClose(files, rows)
}

const filesQueryFilesByIDsQuery = GenericQuery(`
	SELECT
		id,
		rand,
		path,
		modification_time,
		file_size,
		last_seen,
		to_be_read,
		to_be_compared,
		checksum,
		last_read
	FROM {FILES}
		WHERE id IN (?)
	;
`)

func (d *DB) FilesQueryFilesByIDs(ctx context.Context, querier RebindQueryerContext, fileIDs []uint64) (*sqlx.Rows, error) {
	if querier == nil {
		querier = &d.DB
	}

	query, args, err := sqlx.In(filesQueryFilesByIDsQuery.SubstituteAll(d), fileIDs)
	if err != nil {
		return nil, err
	}

	// query is a generic query using `?` as the bindvar.
	// It needs to be rebound to match the backend in use.
	query = querier.Rebind(query)

	return querier.QueryxContext(ctx, query, args...)
}

func (d *DB) FilesFetchFilesByIDs(ctx context.Context, querier RebindQueryerContext, fileIDs []uint64) ([]File, error) {
	files := make([]File, 0, len(fileIDs))

	return d.FilesAppendFilesByIDs(files, ctx, querier, fileIDs)
}

func (d *DB) FilesAppendFilesByIDs(files []File, ctx context.Context, querier RebindQueryerContext, fileIDs []uint64) ([]File, error) {
	baseInd := len(files)

	ind := baseInd

	for i := 0; i < len(fileIDs); {
		rangeEnd := i + MaxPlaceholders
		if rangeEnd >= len(fileIDs) {
			rangeEnd = len(fileIDs)
		}

		rows, err := d.FilesQueryFilesByIDs(ctx, querier, fileIDs[i:rangeEnd])
		if err != nil {
			return files[:baseInd], err
		}

		for rows.Next() {
			if ind == cap(files) {
				files = append(files, File{})
			} else {
				files = files[:len(files)+1]
			}

			err = rows.StructScan(&files[ind])
			if err != nil {
				_ = rows.Close()
				return files[:baseInd], err
			}

			ind += 1
		}
		if err = rows.Err(); err != nil {
			_ = rows.Close()
			return files[:baseInd], err
		}

		if err = rows.Close(); err != nil {
			return files[:baseInd], err
		}
		i = rangeEnd
	}

	return files, nil
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

const filesToBeReadFetcherFetchQuery = GenericQuery(`
	SELECT
		id, rand, path, file_size
	FROM {FILES}
		WHERE
				to_be_read = '1'
			AND
				(rand > ? OR (rand = ? AND id > ?))
			AND
				rand <= ?
		ORDER BY rand, id ASC
		LIMIT ?
	;
`)

const filesToBeReadFetcherNextChunkQuery = GenericQuery(`
	(
		SELECT
			rand
		FROM {FILES}
			WHERE rand > ?
			ORDER BY rand ASC
			LIMIT ?
	)
		ORDER BY rand DESC LIMIT 1;
`)

type FilesToBeReadFetcherConfig struct {
	ChunkSize uint64
}

type FilesToBeReadFetcher struct {
	db     *DB
	config *FilesToBeReadFetcherConfig
	// it is the ChunkIteratorByRand used to limit all queries to only a range
	// of rows.
	// The chunk size is not enforced accurately, as one rand value may refer to
	// multiple rows.
	it                ChunkIteratorByRand
	chunkContainsRows bool
	lastRand          float64
	lastID            uint64
}

func (f *FilesToBeReadFetcher) initialise() {
	f.it = ChunkIteratorByRand{
		ChunkSize:      f.config.ChunkSize,
		NextChunkQuery: filesToBeReadFetcherNextChunkQuery.SubstituteAll(f.db),
	}
	f.lastRand = -1
	f.lastID = 0
}

func (f *FilesToBeReadFetcher) queryFilesToBeReadPaginated(ctx context.Context, querier sqlx.QueryerContext, limit uint64) (*sqlx.Rows, error) {
	if querier == nil {
		querier = &f.db.DB
	}

	return querier.QueryxContext(
		ctx,
		filesToBeReadFetcherFetchQuery.SubstituteAll(f.db),
		f.lastRand,
		f.lastRand,
		f.lastID,
		f.it.LastRand(),
		limit,
	)
}

func (f *FilesToBeReadFetcher) advanceToNextChunk(ctx context.Context, querier sqlx.QueryerContext) (bool, error) {
	if querier == nil {
		querier = &f.db.DB
	}

	ok := f.it.Next(ctx, querier)
	if !ok {
		if f.it.Err() != nil {
			return false, f.it.Err()
		}
		// no more chunks, chunkIterator exhausted
		return false, nil
	}

	f.chunkContainsRows = true
	return true, nil
}

// AppendNext fetches the next batch of rows representing files to be read
// from the database. The rows are appended to the files slice.
//
// AppendNext attempts to fetch exactly limit rows. If less than limit Files
// are appended to the passed in files slice, the end of the table has been
// reached.
func (f *FilesToBeReadFetcher) AppendNext(files []File, ctx context.Context, querier sqlx.QueryerContext, limit uint64) ([]File, error) {
	if f.it.NextChunkQuery == "" {
		f.initialise()
	}

	queryLimit := limit

	for {
		baseInd := len(files)

		if !f.chunkContainsRows {
			ok, err := f.advanceToNextChunk(ctx, querier)
			if err != nil {
				return files[:baseInd], err
			} else if !ok {
				break
			}
		}

		rows, err := f.queryFilesToBeReadPaginated(ctx, querier, queryLimit)
		if err != nil {
			return files[:baseInd], err
		}
		files, err = filesAppendFromRowsAndClose(files, rows)
		if err != nil {
			return files[:baseInd], err
		}

		if len(files[baseInd:]) > 0 {
			// at least one file was fetched
			f.lastRand = files[len(files)-1].Rand
			f.lastID = files[len(files)-1].ID
		}

		if uint64(len(files[baseInd:])) < queryLimit {
			// not enough files returned as chunk is exhausted, try to fetch
			// files from next chunk
			queryLimit = limit - uint64(len(files[baseInd:]))
			f.chunkContainsRows = false
			continue
		} else {
			break
		}
	}

	return files, nil
}

// FetchNext fetches the next batch of rows representing files to be read from
// the database.
//
// FetchNext attempts to fetch exactly limit rows. If the length of the
// returned Files slice is less than limit, the end of the table has been
// reached.
func (f *FilesToBeReadFetcher) FetchNext(ctx context.Context, querier sqlx.QueryerContext, limit uint64) ([]File, error) {
	return f.AppendNext(nil, ctx, querier, limit)
}

func (d *DB) FilesToBeReadFetcher(config *FilesToBeReadFetcherConfig) FilesToBeReadFetcher {
	return FilesToBeReadFetcher{
		db:     d,
		config: config,
	}
}
