package meda

import (
	"context"
	"errors"

	"github.com/jmoiron/sqlx"
)

const dbLockTableNameBase = "db_lock"

func (d *DB) DBLockTableName() string {
	return d.Config.TablePrefix + dbLockTableNameBase
}

const dbLockCreateTableQuery = GenericQuery(`
	CREATE TABLE IF NOT EXISTS {DBLOCK} (
		id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`)

func (d *DB) dbLockCreateTable(ctx context.Context) error {
	_, err := d.ExecContext(ctx, dbLockCreateTableQuery.SubstituteAll(d))
	return err
}

// Error variables related to DBLockLocker.
var (
	ErrAlreadyLocked = errors.New("locker already holds db lock")
	ErrNotLocked     = errors.New("locker does not hold db lock")
)

type DBLockLocker struct {
	ctx context.Context
	db  *DB
	tx  *sqlx.Tx
}

func (l *DBLockLocker) IsLocked() bool {
	return l.tx != nil
}

var lockLockTableQuery = GenericQuery(`
	LOCK TABLES
		{DBLOCK} WRITE
	;
`)

func (l *DBLockLocker) Lock(ctx context.Context) error {
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

func (l *DBLockLocker) Unlock(ctx context.Context) error {
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

func (d *DB) DBLockLocker(ctx context.Context) DBLockLocker {
	return DBLockLocker{
		ctx: ctx,
		db:  d,
	}
}
