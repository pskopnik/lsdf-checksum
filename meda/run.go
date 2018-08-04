package meda

import (
	_ "github.com/jmoiron/sqlx"
)

const RunsTableName = "runs"

type Run struct {
	Id           uint64 `db:"id"`
	SnapshotName string `db:"snapshot_name"`
	SnapshotId   uint64 `db:"snapshot_id"`
	RunAt        Time   `db:"run_at"`
}
