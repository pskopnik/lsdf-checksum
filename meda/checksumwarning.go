package meda

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
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
	return errors.Wrap(err, "(*DB).checksumWarningsCreateTable")
}

type ChecksumWarning struct {
	ID               uint64 `db:"id"`
	FileID           uint64 `db:"file_id"`
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

	stmt, err := preparer.PrepareNamedContext(ctx, checksumWarningsPrepareInsertQuery.SubstituteAll(d))
	if err != nil {
		return nil, errors.Wrap(err, "(*DB).ChecksumWarningsPrepareInsert")
	}

	return stmt, nil
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
			return checksumWarnings[:baseInd], errors.Wrap(err, "checksumWarningsAppendFromRows: scan row into struct")
		}

		i += 1
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return checksumWarnings[:baseInd], errors.Wrap(err, "checksumWarningsAppendFromRows: iterate over rows")
	}

	if err = rows.Close(); err != nil {
		return checksumWarnings[:baseInd], errors.Wrap(err, "checksumWarningsAppendFromRows: close rows")
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

	rows, err := querier.QueryxContext(ctx, checksumWarningsQueryAllQuery.SubstituteAll(d))
	if err != nil {
		return nil, errors.Wrap(err, "(*DB).ChecksumWarningsQueryAll")
	}

	return rows, nil
}

func (d *DB) ChecksumWarningsFetchAll(ctx context.Context, querier sqlx.QueryerContext) ([]ChecksumWarning, error) {
	warnings, err := d.ChecksumWarningsAppendAll(nil, ctx, querier)
	if err != nil {
		return nil, errors.Wrap(err, "(*DB).ChecksumWarningsFetchAll")
	}

	return warnings, nil
}

func (d *DB) ChecksumWarningsAppendAll(checksumWarnings []ChecksumWarning, ctx context.Context, querier sqlx.QueryerContext) ([]ChecksumWarning, error) {
	rows, err := d.ChecksumWarningsQueryAll(ctx, querier)
	if err != nil {
		return checksumWarnings, errors.Wrap(err, "(*DB).ChecksumWarningsAppendAll")
	}

	checksumWarnings, err = checksumWarningsAppendFromRows(checksumWarnings, rows)
	if err != nil {
		return checksumWarnings, errors.Wrap(err, "(*DB).ChecksumWarningsAppendAll")
	}

	return checksumWarnings, nil
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

	rows, err := querier.QueryxContext(ctx, checksumWarningsQueryFromLastNRunsQuery.SubstituteAll(d), n)
	if err != nil {
		return nil, errors.Wrapf(err, "(*DB).ChecksumWarningsQueryFromLastNRuns: n = %d", n)
	}

	return rows, nil
}

func (d *DB) ChecksumWarningsFetchFromLastNRuns(ctx context.Context, querier sqlx.QueryerContext, n uint64) ([]ChecksumWarning, error) {
	warnings, err := d.ChecksumWarningsAppendFromLastNRuns(nil, ctx, querier, n)
	if err != nil {
		return warnings, errors.Wrap(err, "(*DB).ChecksumWarningsFetchFromLastNRuns")
	}

	return warnings, nil
}

func (d *DB) ChecksumWarningsAppendFromLastNRuns(checksumWarnings []ChecksumWarning, ctx context.Context, querier sqlx.QueryerContext, n uint64) ([]ChecksumWarning, error) {
	rows, err := d.ChecksumWarningsQueryFromLastNRuns(ctx, querier, n)
	if err != nil {
		return checksumWarnings, errors.Wrap(err, "(*DB).ChecksumWarningsAppendFromLastNRuns")
	}

	checksumWarnings, err = checksumWarningsAppendFromRows(checksumWarnings, rows)
	if err != nil {
		return checksumWarnings, errors.Wrap(err, "(*DB).ChecksumWarningsAppendFromLastNRuns")
	}

	return checksumWarnings, nil
}

const checksumWarningsQueryFromRunByIDQuery = GenericQuery(`
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

func (d *DB) ChecksumWarningsQueryFromRunByID(ctx context.Context, querier sqlx.QueryerContext, runID uint64) (*sqlx.Rows, error) {
	if querier == nil {
		querier = &d.DB
	}

	rows, err := querier.QueryxContext(ctx, checksumWarningsQueryFromRunByIDQuery.SubstituteAll(d), runID)
	if err != nil {
		return nil, errors.Wrapf(err, "(*DB).ChecksumWarningsQueryFromRunByID: runID = %d", runID)
	}

	return rows, nil
}

func (d *DB) ChecksumWarningsFetchFromRunByID(ctx context.Context, querier sqlx.QueryerContext, runID uint64) ([]ChecksumWarning, error) {
	checksumWarnings, err := d.ChecksumWarningsAppendFromRunByID(nil, ctx, querier, runID)
	if err != nil {
		return checksumWarnings, errors.Wrap(err, "(*DB).ChecksumWarningsFetchFromRunByID")
	}

	return checksumWarnings, nil
}

func (d *DB) ChecksumWarningsAppendFromRunByID(checksumWarnings []ChecksumWarning, ctx context.Context, querier sqlx.QueryerContext, runID uint64) ([]ChecksumWarning, error) {
	rows, err := d.ChecksumWarningsQueryFromRunByID(ctx, querier, runID)
	if err != nil {
		return checksumWarnings, errors.Wrap(err, "(*DB).ChecksumWarningsAppendFromRunByID")
	}

	checksumWarnings, err = checksumWarningsAppendFromRows(checksumWarnings, rows)
	if err != nil {
		return checksumWarnings, errors.Wrap(err, "(*DB).ChecksumWarningsAppendFromRunByID")
	}

	return checksumWarnings, nil
}

const checksumWarningsDeleteByIDQuery = GenericQuery(`
	DELETE FROM {CHECKSUM_WARNINGS}
		WHERE id IN (?)
	;
`)

func (d *DB) ChecksumWarningsDeleteByID(ctx context.Context, execer RebindExecerContext, checksumWarningIDs []uint64) (sql.Result, error) {
	if execer == nil {
		execer = &d.DB
	}

	query, args, err := sqlx.In(checksumWarningsDeleteByIDQuery.SubstituteAll(d), checksumWarningIDs)
	if err != nil {
		return nil, errors.Wrap(err, "(*DB).ChecksumWarningsDeleteByID: expand query")
	}

	// query is a generic query using `?` as the bindvar.
	// It needs to be rebound to match the backend in use.
	query = execer.Rebind(query)

	res, err := execer.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, errors.Wrap(err, "(*DB).ChecksumWarningsDeleteByID: exec query")
	}

	return res, nil
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
	var checksumWarningIDs []uint64
	var totalRowsAffected int64

	for i := 0; i < len(checksumWarnings); {
		rangeEnd := i + MaxPlaceholders
		if rangeEnd >= len(checksumWarnings) {
			rangeEnd = len(checksumWarnings)
		}
		checksumWarningIDs = append(checksumWarningIDs[:0], make([]uint64, rangeEnd-i)...)

		for ind := range checksumWarnings[i:rangeEnd] {
			checksumWarningIDs[ind] = checksumWarnings[i+ind].ID
		}

		res, err := d.ChecksumWarningsDeleteByID(ctx, execer, checksumWarningIDs)
		if rowsAffected, err := res.RowsAffected(); err == nil {
			totalRowsAffected += rowsAffected
		}
		if err != nil {
			return rowsAffectedResult(totalRowsAffected), errors.Wrap(err, "(*DB).ChecksumWarningsDeleteChecksumWarnings")
		}

		i = rangeEnd
	}

	return rowsAffectedResult(totalRowsAffected), nil
}
