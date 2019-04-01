package meda

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var _ NamedPreparer = &sqlx.DB{}
var _ NamedPreparer = &sqlx.Tx{}

type NamedPreparer interface {
	PrepareNamed(query string) (*sqlx.NamedStmt, error)
}

var _ NamedExecerContext = &sqlx.DB{}
var _ NamedExecerContext = &sqlx.Tx{}

type NamedExecerContext interface {
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
}

var _ NamedPreparerContext = &sqlx.DB{}
var _ NamedPreparerContext = &sqlx.Tx{}

type NamedPreparerContext interface {
	PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error)
}

var _ RebindQueryerContext = &sqlx.DB{}
var _ RebindQueryerContext = &sqlx.Tx{}

type RebindQueryerContext interface {
	sqlx.QueryerContext

	Rebind(query string) string
}

var _ RebindExecerContext = &sqlx.DB{}
var _ RebindExecerContext = &sqlx.Tx{}

type RebindExecerContext interface {
	sqlx.ExecerContext

	Rebind(query string) string
}

// Time is an alias to time.Time with additional methods defined to be parsable
// by SQL database drivers.
//
// The implementation is powered by github.com/go-sql-driver/mysql.NullTime.
type Time time.Time

func (t *Time) Scan(value interface{}) (err error) {
	nt := mysql.NullTime{}
	err = nt.Scan(value)
	if err != nil {
		return
	}

	*t = Time(nt.Time)

	return
}

func (t Time) Value() (driver.Value, error) {
	return time.Time(t), nil
}

// NullUint64 provides a uint64 which may be null. This type works analogously
// to the Null... types in the database/sql package.
//
// The implementation is powered by database/sql.NullInt64.
// This type has not been tested for uint64 values which are not valid int64
// values! However, the most common mysql database driver does not support
// uint64 at all.
//
//     https://github.com/go-sql-driver/mysql/issues/715
type NullUint64 struct {
	Uint64 uint64
	Valid  bool
}

func (n *NullUint64) Scan(value interface{}) (err error) {
	nint64 := sql.NullInt64{}
	err = nint64.Scan(value)
	if err != nil {
		return
	}

	n.Uint64, n.Valid = uint64(nint64.Int64), nint64.Valid

	return
}

func (n NullUint64) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return int64(n.Uint64), nil
}
