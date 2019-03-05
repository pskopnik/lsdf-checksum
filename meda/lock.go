package meda

import (
	"context"
)

const lockTableNameBase = "db_lock"

func (d *DB) LockTableName() string {
	return d.Config.TablePrefix + lockTableNameBase
}

const lockCreateTableQuery = GenericQuery(`
	CREATE TABLE IF NOT EXISTS {LOCK} (
		id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`)

func (d *DB) lockCreateTable(ctx context.Context) error {
	_, err := d.ExecContext(ctx, lockCreateTableQuery.SubstituteAll(d))
	return err
}
