package meda

import (
	"context"
	"errors"

	"github.com/jmoiron/sqlx"
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

// Error variables related to LockLocker.
var (
	ErrAlreadyLocked = errors.New("locker already holds lock")
	ErrNotLocked     = errors.New("locker does not hold lock")
)

type LockLocker struct {
	ctx context.Context
	db  *DB
	tx  *sqlx.Tx
}

func (l *LockLocker) IsLocked() bool {
	return l.tx != nil
}

var lockLockTableQuery = GenericQuery(`
	LOCK TABLES
		{LOCK} WRITE
	;
`)

func (l *LockLocker) Lock(ctx context.Context) error {
	if l.IsLocked() {
		return ErrAlreadyLocked
	}

	tx, err := l.db.BeginTxx(l.ctx, nil)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, lockLockTableQuery.SubstituteAll(l.db))
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	l.tx = tx

	return nil
}

var lockUnlockTablesQuery = GenericQuery(`
	UNLOCK TABLES;
`)

func (l *LockLocker) Unlock(ctx context.Context) error {
	if !l.IsLocked() {
		return ErrNotLocked
	}

	tx := l.tx
	l.tx = nil

	_, err := tx.ExecContext(ctx, lockUnlockTablesQuery.SubstituteAll(l.db))
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (d *DB) LockLocker(ctx context.Context) LockLocker {
	return LockLocker{
		ctx: ctx,
		db:  d,
	}
}
