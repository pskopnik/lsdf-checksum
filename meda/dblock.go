package meda

import (
	"context"
	"errors"

	"github.com/jmoiron/sqlx"
	pkgErrors "github.com/pkg/errors"
)

const dbLockTableNameBase = "dblock"

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
	if err != nil {
		return pkgErrors.Wrap(err, "(*DB).dbLockCreateTable")
	}

	return nil
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

var dbLockLockerLockQuery = GenericQuery(`
	LOCK TABLES
		{DBLOCK} WRITE
	;
`)

func (l *DBLockLocker) Lock(ctx context.Context) error {
	if l.IsLocked() {
		return pkgErrors.Wrap(ErrAlreadyLocked, "(*DBLockLocker).Lock")
	}

	tx, err := l.db.BeginTxx(l.ctx, nil)
	if err != nil {
		return pkgErrors.Wrap(err, "(*DBLockLocker).Lock: begin transaction")
	}

	_, err = tx.ExecContext(ctx, dbLockLockerLockQuery.SubstituteAll(l.db))
	if err != nil {
		_ = tx.Rollback()
		return pkgErrors.Wrap(err, "(*DBLockLocker).Lock: exec lock query")
	}

	l.tx = tx

	return nil
}

var dbLockLockerUnlockQuery = GenericQuery(`
	UNLOCK TABLES;
`)

func (l *DBLockLocker) Unlock(ctx context.Context) error {
	if !l.IsLocked() {
		return pkgErrors.Wrap(ErrNotLocked, "(*DBLockLocker).Unlock")
	}

	tx := l.tx
	l.tx = nil

	_, err := tx.ExecContext(ctx, dbLockLockerUnlockQuery.SubstituteAll(l.db))
	if err != nil {
		_ = tx.Rollback()
		return pkgErrors.Wrap(err, "(*DBLockLocker).Unlock: exec unlock query")
	}

	err = tx.Commit()
	if err != nil {
		return pkgErrors.Wrap(err, "(*DBLockLocker).Unlock: commit transaction")
	}

	return nil
}

func (d *DB) DBLockLocker(ctx context.Context) DBLockLocker {
	return DBLockLocker{
		ctx: ctx,
		db:  d,
	}
}
