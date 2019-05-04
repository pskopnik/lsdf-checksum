package meda

import (
	"context"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"
	pkgErrors "github.com/pkg/errors"
	"gopkg.in/tomb.v2"
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

	tx *sqlx.Tx

	tomb   *tomb.Tomb
	cancel context.CancelFunc
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
	tombCtx, cancel := context.WithCancel(l.ctx)
	l.tomb, _ = tomb.WithContext(tombCtx)
	l.cancel = cancel

	l.tomb.Go(l.keepAliver)

	return nil
}

var dbLockLockerUnlockQuery = GenericQuery(`
	UNLOCK TABLES;
`)

func (l *DBLockLocker) Unlock(ctx context.Context) error {
	if !l.IsLocked() {
		return pkgErrors.Wrap(ErrNotLocked, "(*DBLockLocker).Unlock")
	}

	l.cancel()

	select {
	case <-l.tomb.Dead():
	case <-ctx.Done():
		return pkgErrors.Wrap(ctx.Err(), "(*DBLockLocker).Unlock: context done while waiting for keepAliver death")
	}

	if l.tomb.Err() != nil && pkgErrors.Cause(l.tomb.Err()) != context.Canceled {
		return pkgErrors.Wrap(l.tomb.Err(), "(*DBLockLocker).Unlock: keepAliver died with error")
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

// Wait blocks until all background workers (the keepAliver) have died. Then
// the same error is returned as by Err.
// Wait is only valid if l is locked.
func (l *DBLockLocker) Wait() error {
	return l.tomb.Wait()
}

// Dead returns a channel closed as soon as all background workers (the
// keepAliver) have died.
// Dead is only valid if l is locked.
func (l *DBLockLocker) Dead() <-chan struct{} {
	return l.tomb.Dead()
}

// Err returns the error because of which the background workers (the
// keepAliver) are shutting down.
// Err is only valid if l is locked.
func (l *DBLockLocker) Err() error {
	return l.tomb.Err()
}

func (l *DBLockLocker) keepAliver() error {
	dying := l.tomb.Dying()
	timer := time.NewTimer(0)

	// Exhaust timer
	if !timer.Stop() {
		<-timer.C
	}

	for {
		timer.Reset(l.db.Config.LockKeepAliveInterval)

		select {
		case <-timer.C:
			err := l.querySelectOne(l.tomb.Context(nil))
			if err != nil {
				return pkgErrors.Wrap(err, "(*DBLockLocker).keepAliver")
			}
		case <-dying:
			// Exhaust timer
			if !timer.Stop() {
				<-timer.C
			}

			return tomb.ErrDying
		}
	}

	return nil
}

var dbLockLockerQuerySelectOneQuery = GenericQuery(`
	SELECT 1;
`)

func (l *DBLockLocker) querySelectOne(ctx context.Context) error {
	var one uint64

	err := l.tx.QueryRowxContext(ctx, dbLockLockerQuerySelectOneQuery.SubstituteAll(l.db)).Scan(&one)
	if err != nil {
		return pkgErrors.Wrap(err, "(*DBLockLocker).querySelectOne")
	}

	return nil
}

func (d *DB) DBLockLocker(ctx context.Context) DBLockLocker {
	return DBLockLocker{
		ctx: ctx,
		db:  d,
	}
}
