package meda

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
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
	return err
}

type Run struct {
	Id           uint64         `db:"id"`
	SnapshotName sql.NullString `db:"snapshot_name"`
	SnapshotId   NullUint64     `db:"snapshot_id"`
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

func (d *DB) RunsInsert(ctx context.Context, execer sqlx.ExecerContext, run *Run) (sql.Result, error) {
	if execer == nil {
		execer = &d.DB
	}

	return execer.ExecContext(ctx, runsInsertQuery.SubstituteAll(d), run)
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

func (d *DB) RunsUpdate(ctx context.Context, execer sqlx.ExecerContext, run *Run) (sql.Result, error) {
	if execer == nil {
		execer = &d.DB
	}

	return execer.ExecContext(ctx, runsUpdateQuery.SubstituteAll(d), run)
}

const runsQueryLatestQuery = GenericQuery(`
	SELECT
		id, snapshot_name, snapshot_id, run_at, sync_mode, state
	FROM {RUNS}
		ORDER BY id DESC
		LIMIT 1
	;
`)

func (d *DB) RunsQueryLatest(ctx context.Context, querier sqlx.QueryerContext) (*Run, error) {
	if querier == nil {
		querier = &d.DB
	}

	var run Run

	err := querier.QueryRowxContext(ctx, runsQueryLatestQuery.SubstituteAll(d)).StructScan(&run)
	if err != nil {
		return nil, err
	}

	return &run, nil
}

const runsQueryAllQuery = GenericQuery(`
	SELECT
		id, snapshot_name, snapshot_id, run_at, sync_mode, state
	FROM {RUNS}
	;
`)

func (d *DB) RunsQueryAll(ctx context.Context, querier sqlx.QueryerContext) (*sqlx.Rows, error) {
	if querier == nil {
		querier = &d.DB
	}

	return querier.QueryxContext(ctx, runsQueryAllQuery.SubstituteAll(d))
}

var (
	ErrInvalidRunSyncModeValueType = errors.New("Invalid RunSyncMode value type.")
	ErrInvalidRunStateValueType = errors.New("Invalid RunState value type.")
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
	strSrc, ok := src.(string)
	if !ok {
		return ErrInvalidRunSyncModeValueType
	}

	switch strSrc {
	case "full":
		*r = RSMFull
	case "incremental":
		*r = RSMIncremental
	case "default":
		*r = RSMDefault
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
	strSrc, ok := src.(string)
	if !ok {
		return ErrInvalidRunStateValueType
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
	case "invalid":
		*r = RSInvalid
	default:
		*r = RSInvalid
	}

	return nil
}
