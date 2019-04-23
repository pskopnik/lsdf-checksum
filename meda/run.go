package meda

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	pkgErrors "github.com/pkg/errors"
)

const runsTableNameBase = "runs"

func (d *DB) RunsTableName() string {
	return d.Config.TablePrefix + runsTableNameBase
}

const runsCreateTableQuery = GenericQuery(`
	CREATE TABLE IF NOT EXISTS {RUNS} (
		id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
		snapshot_name varchar(256) DEFAULT NULL,
		snapshot_id bigint(20) unsigned DEFAULT NULL,
		run_at datetime(6) DEFAULT NULL,
		sync_mode varchar(20) NOT NULL,
		state varchar(20) NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`)

func (d *DB) runsCreateTable(ctx context.Context) error {
	_, err := d.ExecContext(ctx, runsCreateTableQuery.SubstituteAll(d))
	if err != nil {
		return pkgErrors.Wrap(err, "(*DB).runsCreateTable")
	}

	return nil
}

type Run struct {
	ID           uint64         `db:"id"`
	SnapshotName sql.NullString `db:"snapshot_name"`
	SnapshotID   NullUint64     `db:"snapshot_id"`
	RunAt        mysql.NullTime `db:"run_at"`
	SyncMode     RunSyncMode    `db:"sync_mode"`
	State        RunState       `db:"state"`
}

const runsInsertQuery = GenericQuery(`
	INSERT INTO {RUNS} (
			snapshot_name, snapshot_id, run_at, sync_mode, state
		) VALUES (
			:snapshot_name, :snapshot_id, :run_at, :sync_mode, :state
		)
	;
`)

func (d *DB) RunsInsertAndSetID(ctx context.Context, execer NamedExecerContext, run *Run) (sql.Result, error) {
	if execer == nil {
		execer = &d.DB
	}

	result, err := execer.NamedExecContext(ctx, runsInsertQuery.SubstituteAll(d), run)
	if err != nil {
		return nil, pkgErrors.Wrap(err, "(*DB).RunsInsertAndSetID: exec insert query")
	}

	id, err := result.LastInsertId()
	if err != nil {
		return nil, pkgErrors.Wrap(err, "(*DB).RunsInsertAndSetID: retrieve last insert ID")
	}

	run.ID = uint64(id)

	return result, nil
}

const runsUpdateQuery = GenericQuery(`
	UPDATE {RUNS}
		SET
			snapshot_name = :snapshot_name,
			snapshot_id = :snapshot_id,
			run_at = :run_at,
			sync_mode = :sync_mode,
			state = :state
		WHERE
			id = :id
	;
`)

func (d *DB) RunsUpdate(ctx context.Context, execer NamedExecerContext, run *Run) (sql.Result, error) {
	if execer == nil {
		execer = &d.DB
	}

	res, err := execer.NamedExecContext(ctx, runsUpdateQuery.SubstituteAll(d), run)
	if err != nil {
		return nil, pkgErrors.Wrap(err, "(*DB).RunsUpdate")
	}

	return res, nil
}

const runsQueryByIDQuery = GenericQuery(`
	SELECT
		id, snapshot_name, snapshot_id, run_at, sync_mode, state
	FROM {RUNS}
		WHERE id = ?
		LIMIT 1
	;
`)

func (d *DB) RunsQueryByID(ctx context.Context, querier sqlx.QueryerContext, id uint64) (Run, error) {
	if querier == nil {
		querier = &d.DB
	}

	var run Run

	err := querier.QueryRowxContext(ctx, runsQueryByIDQuery.SubstituteAll(d), id).StructScan(&run)
	if err != nil {
		return Run{}, pkgErrors.Wrap(err, "(*DB).RunsQueryByID")
	}

	return run, nil
}

func runsAppendFromRows(runs []Run, rows *sqlx.Rows) ([]Run, error) {
	baseInd := len(runs)

	var err error
	i := baseInd

	for rows.Next() {
		if i == cap(runs) {
			runs = append(runs, Run{})
		} else {
			runs = runs[:len(runs)+1]
		}

		err = rows.StructScan(&runs[i])
		if err != nil {
			_ = rows.Close()
			return runs[:baseInd], pkgErrors.Wrap(err, "runsAppendFromRows: scan row into struct")
		}

		i += 1
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return runs[:baseInd], pkgErrors.Wrap(err, "runsAppendFromRows: iterate over rows")
	}

	if err = rows.Close(); err != nil {
		return runs[:baseInd], pkgErrors.Wrap(err, "runsAppendFromRows: close rows")
	}

	return runs, nil
}

const runsQueryAllQuery = GenericQuery(`
	SELECT
		id, snapshot_name, snapshot_id, run_at, sync_mode, state
	FROM {RUNS}
		ORDER BY id ASC
	;
`)

func (d *DB) RunsQueryAll(ctx context.Context, querier sqlx.QueryerContext) (*sqlx.Rows, error) {
	if querier == nil {
		querier = &d.DB
	}

	rows, err := querier.QueryxContext(ctx, runsQueryAllQuery.SubstituteAll(d))
	if err != nil {
		return nil, pkgErrors.Wrap(err, "(*DB).RunsQueryAll")
	}

	return rows, nil
}

func (d *DB) RunsFetchAll(ctx context.Context, querier sqlx.QueryerContext) ([]Run, error) {
	runs, err := d.RunsAppendAll(nil, ctx, querier)
	if err != nil {
		return nil, pkgErrors.Wrap(err, "(*DB).RunsFetchAll")
	}

	return runs, nil
}

func (d *DB) RunsAppendAll(runs []Run, ctx context.Context, querier sqlx.QueryerContext) ([]Run, error) {
	rows, err := d.RunsQueryAll(ctx, querier)
	if err != nil {
		return runs, pkgErrors.Wrap(err, "(*DB).RunsAppendAll")
	}

	runs, err = runsAppendFromRows(runs, rows)
	if err != nil {
		return runs, pkgErrors.Wrap(err, "(*DB).RunsAppendAll")
	}

	return runs, nil
}

const runsQueryLastNQuery = GenericQuery(`
	SELECT
		id, snapshot_name, snapshot_id, run_at, sync_mode, state
	FROM (
		SELECT
			id, snapshot_name, snapshot_id, run_at, sync_mode, state
		FROM {RUNS}
			ORDER BY id DESC
			LIMIT ?
	) as last_runs
		ORDER BY id ASC
	;
`)

func (d *DB) RunsQueryLastN(ctx context.Context, querier sqlx.QueryerContext, n uint64) (*sqlx.Rows, error) {
	if querier == nil {
		querier = &d.DB
	}

	rows, err := querier.QueryxContext(ctx, runsQueryLastNQuery.SubstituteAll(d), n)
	if err != nil {
		return nil, pkgErrors.Wrap(err, "(*DB).RunsQueryLastN")
	}

	return rows, nil
}

func (d *DB) RunsFetchLastN(ctx context.Context, querier sqlx.QueryerContext, n uint64) ([]Run, error) {
	runs, err := d.RunsAppendLastN(nil, ctx, querier, n)
	if err != nil {
		return nil, pkgErrors.Wrap(err, "(*DB).RunsFetchLastN")
	}

	return runs, nil
}

func (d *DB) RunsAppendLastN(runs []Run, ctx context.Context, querier sqlx.QueryerContext, n uint64) ([]Run, error) {
	rows, err := d.RunsQueryLastN(ctx, querier, n)
	if err != nil {
		return runs, pkgErrors.Wrap(err, "(*DB).RunsAppendLastN")
	}

	runs, err = runsAppendFromRows(runs, rows)
	if err != nil {
		return runs, pkgErrors.Wrap(err, "(*DB).RunsAppendLastN")
	}

	return runs, nil
}

const runsQueryIncompleteQuery = GenericQuery(`
	SELECT
		id, snapshot_name, snapshot_id, run_at, sync_mode, state
	FROM {RUNS}
		WHERE state NOT IN ("finished", "aborted")
		ORDER BY id ASC
	;
`)

func (d *DB) RunsQueryIncomplete(ctx context.Context, querier sqlx.QueryerContext) (*sqlx.Rows, error) {
	if querier == nil {
		querier = &d.DB
	}

	rows, err := querier.QueryxContext(ctx, runsQueryIncompleteQuery.SubstituteAll(d))
	if err != nil {
		return nil, pkgErrors.Wrap(err, "(*DB).RunsQueryIncomplete")
	}

	return rows, nil
}

func (d *DB) RunsFetchIncomplete(ctx context.Context, querier sqlx.QueryerContext) ([]Run, error) {
	runs, err := d.RunsAppendIncomplete(nil, ctx, querier)
	if err != nil {
		return nil, pkgErrors.Wrap(err, "(*DB).RunsFetchIncomplete")
	}

	return runs, nil
}

func (d *DB) RunsAppendIncomplete(runs []Run, ctx context.Context, querier sqlx.QueryerContext) ([]Run, error) {
	rows, err := d.RunsQueryIncomplete(ctx, querier)
	if err != nil {
		return runs, pkgErrors.Wrap(err, "(*DB).RunsAppendIncomplete")
	}

	runs, err = runsAppendFromRows(runs, rows)
	if err != nil {
		return runs, pkgErrors.Wrap(err, "(*DB).RunsAppendIncomplete")
	}

	return runs, nil
}

const runsExistsIncompleteBeforeIDQuery = GenericQuery(`
	SELECT
		EXISTS (
			SELECT 1
			FROM {RUNS}
				WHERE
						id < ?
					AND
						state NOT IN ("finished", "aborted")
		) AS row_exists
	;
`)

func (d *DB) RunsExistsIncompleteBeforeID(ctx context.Context, querier sqlx.QueryerContext, id uint64) (bool, error) {
	if querier == nil {
		querier = &d.DB
	}

	row := querier.QueryRowxContext(ctx, runsExistsIncompleteBeforeIDQuery.SubstituteAll(d), id)

	var rowExists int

	err := row.Scan(&rowExists)
	if err != nil {
		return false, pkgErrors.Wrap(err, "(*DB).RunsExistsIncompleteBeforeID")
	}

	return rowExists == 1, nil
}

const runsExistsIncompleteIDQuery = GenericQuery(`
	SELECT
		EXISTS (
			SELECT 1
			FROM {RUNS}
				WHERE
					state NOT IN ("finished", "aborted")
		) AS row_exists
	;
`)

func (d *DB) RunsExistsIncomplete(ctx context.Context, querier sqlx.QueryerContext) (bool, error) {
	if querier == nil {
		querier = &d.DB
	}

	row := querier.QueryRowxContext(ctx, runsExistsIncompleteIDQuery.SubstituteAll(d))

	var rowExists int

	err := row.Scan(&rowExists)
	if err != nil {
		return false, pkgErrors.Wrap(err, "(*DB).RunsExistsIncomplete")
	}

	return rowExists == 1, nil
}

// Error variables related to RunSyncMode and RunState.
var (
	ErrInvalidRunSyncModeValueType = errors.New("invalid RunSyncMode value type")
	ErrInvalidRunStateValueType    = errors.New("invalid RunState value type")
)

type RunSyncMode int8

const (
	RSMDefault RunSyncMode = iota
	RSMFull
	RSMIncremental
	RSMInvalid = -1
)

func (r RunSyncMode) String() string {
	switch r {
	case RSMDefault:
		return "default"
	case RSMFull:
		return "full"
	case RSMIncremental:
		return "incremental"
	case RSMInvalid:
		return "invalid"
	default:
		return "invalid"
	}
}

func (r RunSyncMode) Value() (driver.Value, error) {
	return driver.Value(r.String()), nil
}

func (r *RunSyncMode) Scan(src interface{}) error {
	var strSrc string

	switch typedSrc := src.(type) {
	case string:
		strSrc = typedSrc
	case []byte:
		strSrc = string(typedSrc)
	default:
		return pkgErrors.Wrapf(ErrInvalidRunSyncModeValueType, "(*RunSyncMode).Scan: src has type %T", src)
	}

	switch strSrc {
	case "default":
		*r = RSMDefault
	case "full":
		*r = RSMFull
	case "incremental":
		*r = RSMIncremental
	case "invalid":
		*r = RSMInvalid
	default:
		*r = RSMInvalid
	}

	return nil
}

type RunState int8

const (
	RSNil RunState = iota
	RSInitialised
	RSSnapshot
	RSMedasync
	RSWorkqueue
	RSCleanup
	RSFinished
	RSAbortingMedasync
	RSAbortingSnapshot
	RSAborted
	RSInvalid = -1
)

func (r RunState) String() string {
	switch r {
	case RSNil:
		return "nil"
	case RSInitialised:
		return "initialised"
	case RSSnapshot:
		return "snapshot"
	case RSMedasync:
		return "medasync"
	case RSWorkqueue:
		return "workqueue"
	case RSCleanup:
		return "cleanup"
	case RSFinished:
		return "finished"
	case RSAbortingMedasync:
		return "aborting-medasync"
	case RSAbortingSnapshot:
		return "aborting-snapshot"
	case RSAborted:
		return "aborted"
	case RSInvalid:
		return "invalid"
	default:
		return "invalid"
	}
}

func (r RunState) Value() (driver.Value, error) {
	return driver.Value(r.String()), nil
}

func (r *RunState) Scan(src interface{}) error {
	var strSrc string

	switch typedSrc := src.(type) {
	case string:
		strSrc = typedSrc
	case []byte:
		strSrc = string(typedSrc)
	default:
		return pkgErrors.Wrapf(ErrInvalidRunStateValueType, "(*RunState).Scan: src has type %T", src)
	}

	switch strSrc {
	case "nil":
		*r = RSNil
	case "initialised":
		*r = RSInitialised
	case "snapshot":
		*r = RSSnapshot
	case "medasync":
		*r = RSMedasync
	case "workqueue":
		*r = RSWorkqueue
	case "cleanup":
		*r = RSCleanup
	case "finished":
		*r = RSFinished
	case "aborting-medasync":
		*r = RSAbortingMedasync
	case "aborting-snapshot":
		*r = RSAbortingSnapshot
	case "aborted":
		*r = RSAborted
	case "invalid":
		*r = RSInvalid
	default:
		*r = RSInvalid
	}

	return nil
}
