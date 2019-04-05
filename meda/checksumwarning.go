package meda

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

const checksumWarningsTableNameBase = "checksum_warnings"

func (d *DB) ChecksumWarningsTableName() string {
	return d.Config.TablePrefix + checksumWarningsTableNameBase
}

const checksumWarningsCreateTableQuery = GenericQuery(`
	CREATE TABLE IF NOT EXISTS {CHECKSUM_WARNINGS} (
		id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
		file_id bigint(20) unsigned NOT NULL,
		path varbinary(4096) NOT NULL,
		modification_time datetime(6) NOT NULL,
		file_size bigint(20) unsigned NOT NULL,
		expected_checksum varbinary(64) NOT NULL,
		actual_checksum varbinary(64) NOT NULL,
		discovered bigint(20) unsigned NOT NULL,
		last_read bigint(20) unsigned NOT NULL,
		created datetime(6) NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`)

func (d *DB) checksumWarningsCreateTable(ctx context.Context) error {
	_, err := d.ExecContext(ctx, checksumWarningsCreateTableQuery.SubstituteAll(d))
	return err
}

type ChecksumWarning struct {
	Id               uint64 `db:"id"`
	FileId           uint64 `db:"file_id"`
	Path             string `db:"path"`
	ModificationTime Time   `db:"modification_time"`
	FileSize         uint64 `db:"file_size"`
	ExpectedChecksum []byte `db:"expected_checksum"`
	ActualChecksum   []byte `db:"actual_checksum"`
	Discovered       uint64 `db:"discovered"`
	LastRead         uint64 `db:"last_read"`
	Created          Time   `db:"created"`
}

const checksumWarningsPrepareInsertQuery = GenericQuery(`
	INSERT INTO {CHECKSUM_WARNINGS} (
			file_id,
			path,
			modification_time,
			file_size,
			expected_checksum,
			actual_checksum,
			discovered,
			last_read,
			created
		) VALUES (
			:file_id,
			:path,
			:modification_time,
			:file_size,
			:expected_checksum,
			:actual_checksum,
			:discovered,
			:last_read,
			:created
		)
	;
`)

func (d *DB) ChecksumWarningsPrepareInsert(ctx context.Context, preparer NamedPreparerContext) (*sqlx.NamedStmt, error) {
	if preparer == nil {
		preparer = &d.DB
	}

	return preparer.PrepareNamedContext(ctx, checksumWarningsPrepareInsertQuery.SubstituteAll(d))
}

func checksumWarningsAppendFromRows(checksumWarnings []ChecksumWarning, rows *sqlx.Rows) ([]ChecksumWarning, error) {
	baseInd := len(checksumWarnings)

	var err error
	i := baseInd

	for rows.Next() {
		if i == cap(checksumWarnings) {
			checksumWarnings = append(checksumWarnings, ChecksumWarning{})
		} else {
			checksumWarnings = checksumWarnings[:len(checksumWarnings)+1]
		}

		err = rows.StructScan(&checksumWarnings[i])
		if err != nil {
			_ = rows.Close()
			return checksumWarnings[:baseInd], err
		}

		i += 1
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return checksumWarnings[:baseInd], err
	}

	if err = rows.Close(); err != nil {
		return checksumWarnings[:baseInd], err
	}

	return checksumWarnings, nil
}

const checksumWarningsQueryAllQuery = GenericQuery(`
	SELECT
		id,
		file_id,
		path,
		modification_time,
		file_size,
		expected_checksum,
		actual_checksum,
		discovered,
		last_read,
		created
	FROM {CHECKSUM_WARNINGS}
		ORDER BY id ASC
	;
`)

func (d *DB) ChecksumWarningsQueryAll(ctx context.Context, querier sqlx.QueryerContext) (*sqlx.Rows, error) {
	if querier == nil {
		querier = &d.DB
	}

	return querier.QueryxContext(ctx, checksumWarningsQueryAllQuery.SubstituteAll(d))
}

func (d *DB) ChecksumWarningsFetchAll(ctx context.Context, querier sqlx.QueryerContext) ([]ChecksumWarning, error) {
	return d.ChecksumWarningsAppendAll(nil, ctx, querier)
}

func (d *DB) ChecksumWarningsAppendAll(checksumWarnings []ChecksumWarning, ctx context.Context, querier sqlx.QueryerContext) ([]ChecksumWarning, error) {
	rows, err := d.ChecksumWarningsQueryAll(ctx, querier)
	if err != nil {
		return checksumWarnings, err
	}

	return checksumWarningsAppendFromRows(checksumWarnings, rows)
}

const checksumWarningsQueryFromLastNRunsQuery = GenericQuery(`
	SELECT
		{CHECKSUM_WARNINGS}.id,
		{CHECKSUM_WARNINGS}.file_id,
		{CHECKSUM_WARNINGS}.path,
		{CHECKSUM_WARNINGS}.modification_time,
		{CHECKSUM_WARNINGS}.file_size,
		{CHECKSUM_WARNINGS}.expected_checksum,
		{CHECKSUM_WARNINGS}.actual_checksum,
		{CHECKSUM_WARNINGS}.discovered,
		{CHECKSUM_WARNINGS}.last_read,
		{CHECKSUM_WARNINGS}.created
	FROM {CHECKSUM_WARNINGS}
		INNER JOIN (
			SELECT
				id
			FROM {RUNS}
				ORDER BY id DESC
				LIMIT ?
		) AS last_runs
			ON {CHECKSUM_WARNINGS}.discovered = last_runs.id
		ORDER BY id ASC
	;
`)

func (d *DB) ChecksumWarningsQueryFromLastNRuns(ctx context.Context, querier sqlx.QueryerContext, n uint64) (*sqlx.Rows, error) {
	if querier == nil {
		querier = &d.DB
	}

	return querier.QueryxContext(ctx, checksumWarningsQueryFromLastNRunsQuery.SubstituteAll(d), n)
}

func (d *DB) ChecksumWarningsFetchFromLastNRuns(ctx context.Context, querier sqlx.QueryerContext, n uint64) ([]ChecksumWarning, error) {
	return d.ChecksumWarningsAppendFromLastNRuns(nil, ctx, querier, n)
}

func (d *DB) ChecksumWarningsAppendFromLastNRuns(checksumWarnings []ChecksumWarning, ctx context.Context, querier sqlx.QueryerContext, n uint64) ([]ChecksumWarning, error) {
	rows, err := d.ChecksumWarningsQueryFromLastNRuns(ctx, querier, n)
	if err != nil {
		return checksumWarnings, err
	}

	return checksumWarningsAppendFromRows(checksumWarnings, rows)
}

const checksumWarningsQueryFromRunByIdQuery = GenericQuery(`
	SELECT
		id,
		file_id,
		path,
		modification_time,
		file_size,
		expected_checksum,
		actual_checksum,
		discovered,
		last_read,
		created
	FROM {CHECKSUM_WARNINGS}
		WHERE discovered = ?
		ORDER BY id ASC
	;
`)

func (d *DB) ChecksumWarningsQueryFromRunById(ctx context.Context, querier sqlx.QueryerContext, runId uint64) (*sqlx.Rows, error) {
	if querier == nil {
		querier = &d.DB
	}

	return querier.QueryxContext(ctx, checksumWarningsQueryFromRunByIdQuery.SubstituteAll(d), runId)
}

func (d *DB) ChecksumWarningsFetchFromRunById(ctx context.Context, querier sqlx.QueryerContext, runId uint64) ([]ChecksumWarning, error) {
	return d.ChecksumWarningsAppendFromRunById(nil, ctx, querier, runId)
}

func (d *DB) ChecksumWarningsAppendFromRunById(checksumWarnings []ChecksumWarning, ctx context.Context, querier sqlx.QueryerContext, runId uint64) ([]ChecksumWarning, error) {
	rows, err := d.ChecksumWarningsQueryFromRunById(ctx, querier, runId)
	if err != nil {
		return checksumWarnings, err
	}

	return checksumWarningsAppendFromRows(checksumWarnings, rows)
}

const checksumWarningsDeleteByIdQuery = GenericQuery(`
	DELETE FROM {CHECKSUM_WARNINGS}
		WHERE id IN (?)
	;
`)

func (d *DB) ChecksumWarningsDeleteById(ctx context.Context, execer RebindExecerContext, checksumWarningIds []uint64) (sql.Result, error) {
	if execer == nil {
		execer = &d.DB
	}

	query, args, err := sqlx.In(checksumWarningsDeleteByIdQuery.SubstituteAll(d), checksumWarningIds)
	if err != nil {
		return nil, err
	}

	// query is a generic query using `?` as the bindvar.
	// It needs to be rebound to match the backend in use.
	query = execer.Rebind(query)

	return execer.ExecContext(ctx, query, args...)
}

var _ sql.Result = rowsAffectedResult(0)

type rowsAffectedResult int64

func (r rowsAffectedResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r rowsAffectedResult) RowsAffected() (int64, error) {
	return int64(r), nil
}

func (d *DB) ChecksumWarningsDeleteChecksumWarnings(ctx context.Context, execer RebindExecerContext, checksumWarnings []ChecksumWarning) (sql.Result, error) {
	var checksumWarningIds []uint64
	var totalRowsAffected int64

	for i := 0; i < len(checksumWarnings); {
		rangeEnd := i + MaxPlaceholders
		if rangeEnd >= len(checksumWarnings) {
			rangeEnd = len(checksumWarnings)
		}
		checksumWarningIds = append(checksumWarningIds[:0], make([]uint64, rangeEnd-i)...)

		for ind := range checksumWarnings[i:rangeEnd] {
			checksumWarningIds[ind] = checksumWarnings[i+ind].Id
		}

		res, err := d.ChecksumWarningsDeleteById(ctx, execer, checksumWarningIds)
		if rowsAffected, err := res.RowsAffected(); err == nil {
			totalRowsAffected += rowsAffected
		}
		if err != nil {
			return rowsAffectedResult(totalRowsAffected), err
		}

		i = rangeEnd
	}

	return rowsAffectedResult(totalRowsAffected), nil
}
