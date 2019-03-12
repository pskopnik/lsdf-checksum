package meda

import (
	"context"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

// MaxPlaceholders is the maximum number of placeholders ('?') allowed in a
// prepared statements for recent versions of MySQL and MariaDB.
const MaxPlaceholders = 65535

//go:generate confions config Config

type Config struct {
	Driver                string
	DataSourceName        string
	TablePrefix           string
	MaxOpenConns          int
	MaxIdleConns          int
	ConnMaxLifetime       time.Duration
	ServerConcurrencyHint int
}

var DefaultConfig = &Config{
	MaxOpenConns:    100,
	MaxIdleConns:    50,
	ConnMaxLifetime: 10 * time.Minute,
}

type DB struct {
	sqlx.DB

	Config *Config

	replacer *strings.Replacer
}

func Open(config *Config) (*DB, error) {
	sqlxDB, err := sqlx.Open(config.Driver, config.DataSourceName)
	if err != nil {
		return nil, err
	}

	db := &DB{
		DB:     *sqlxDB,
		Config: config,
	}

	db.SetMaxOpenConns(config.MaxOpenConns)
	db.SetMaxIdleConns(config.MaxIdleConns)
	db.SetConnMaxLifetime(config.ConnMaxLifetime)

	db.replacer = db.createReplacer()

	return db, nil
}

func (d *DB) createReplacer() *strings.Replacer {
	return strings.NewReplacer(
		"{CHECKSUM_WARNINGS}", d.ChecksumWarningsTableName(),
		"{INSERTS}", d.InsertsTableName(),
		"{FILES}", d.FilesTableName(),
		"{RUNS}", d.RunsTableName(),
		"{DBLOCK}", d.DBLockTableName(),
	)
}

// ServerConcurrency returns an estimated concurrency value of the MySQL /
// MariaDB server. This value is well suited for the number of threads used
// for performing CPU intensive queries.
func (d *DB) ServerConcurrency() (int, error) {
	if d.Config.ServerConcurrencyHint > 0 {
		return d.Config.ServerConcurrencyHint, nil
	}

	return 1, nil
}

const getVersionQuery = `
	SELECT VERSION();
`

// GetVersion queries the MySQL / MariaDB version from the server.
func (d *DB) GetVersion(ctx context.Context) (string, error) {
	var version string

	err := d.QueryRow(getVersionQuery).Scan(&version)
	if err != nil {
		return "", err
	}

	return version, nil
}

// Migrate performs database schema migrations to ensure the database is in the
// state expected by the meda package.
//
// At the moment, this function only creates tables if these don't exist.
// In the future this functionality could be extended to include performing
// more advanced ALTER TABLE commands.
func (d *DB) Migrate(ctx context.Context) error {
	var err error

	err = d.checksumWarningsCreateTable(ctx)
	if err != nil {
		return err
	}

	err = d.filesCreateTable(ctx)
	if err != nil {
		return err
	}

	err = d.insertsCreateTable(ctx)
	if err != nil {
		return err
	}

	err = d.runsCreateTable(ctx)
	if err != nil {
		return err
	}

	err = d.dbLockCreateTable(ctx)
	if err != nil {
		return err
	}

	return nil
}

// GenericQuery is a query string with placeholders instead of table names.
// By using SubstituteAll() the placeholders are replaced by the actual table
// names resulting in a query string suitable for being executed.
type GenericQuery string

func (s GenericQuery) SubstituteAll(db *DB) string {
	return db.replacer.Replace(string(s))
}
