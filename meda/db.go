package meda

import (
	"context"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

//go:generate confions config Config

type Config struct {
	Driver          string
	DataSourceName  string
	TablePrefix     string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
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

func (m *DB) createReplacer() *strings.Replacer {
	return strings.NewReplacer(
		"{CHECKSUM_WARNINGS}", m.ChecksumWarningsTableName(),
		"{INSERTS}", m.InsertsTableName(),
		"{FILES}", m.FilesTableName(),
		"{RUNS}", m.RunsTableName(),
	)
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

	return nil
}

// GenericQuery is a query string with placeholders instead of table names.
// By using SubstituteAll() the placeholders are replaced by the actual table
// names resulting in a query string suitable for being executed.
type GenericQuery string

func (s GenericQuery) SubstituteAll(db *DB) string {
	return db.replacer.Replace(string(s))
}
