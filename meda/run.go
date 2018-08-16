package meda

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"

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
		state varchar(20) NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`)

func (d *DB) runsCreateTable(ctx context.Context) error {
	_, err := d.ExecContext(ctx, runsCreateTableQuery.SubstituteAll(d))
	return err
}

type Run struct {
	Id           uint64 `db:"id"`
	SnapshotName string `db:"snapshot_name"`
	SnapshotId   uint64 `db:"snapshot_id"`
	RunAt        Time   `db:"run_at"`
}
