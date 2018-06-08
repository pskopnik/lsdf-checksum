package meda

import (
	"time"

	_ "github.com/jmoiron/sqlx"
)

const RunsTableName = "runs"

type Run struct {
	Id           int       `db:"id"`
	SnapshotName string    `db:"snapshot_name"`
	SnapshotId   int       `db:"snapshot_id"`
	RunAt        time.Time `db:"run_at"`
}
