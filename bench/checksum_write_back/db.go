package main

import (
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

// MaxPlaceholders is the maximum number of placeholders ('?') allowed in a
// prepared statements for recent versions of MySQL and MariaDB.
const MaxPlaceholders = 65535

type TablesNameBase struct {
	Files    string
	FileData string
}

//go:generate confions config DBConfig

type DBConfig struct {
	Driver          string
	DataSourceName  string
	TablePrefix     string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	TablesNameBase  TablesNameBase
}

var DBDefaultConfig = &DBConfig{
	MaxOpenConns:    100,
	MaxIdleConns:    50,
	ConnMaxLifetime: 10 * time.Minute,
	TablesNameBase: TablesNameBase{
		Files:    "files",
		FileData: "file_data",
	},
}

type DB struct {
	sqlx.DB

	Config *DBConfig

	replacer *strings.Replacer
}

func Open(config *DBConfig) (*DB, error) {
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
		"{FILES}", d.FilesTableName(),
		"{FILE_DATA}", d.FileDataTableName(),
	)
}

// GenericQuery is a query string with placeholders instead of table names.
// By using SubstituteAll() the placeholders are replaced by the actual table
// names resulting in a query string suitable for being executed.
type GenericQuery string

func (s GenericQuery) SubstituteAll(db *DB) string {
	return db.replacer.Replace(string(s))
}

func (d *DB) FileDataTableName() string {
	return d.Config.TablePrefix + d.Config.TablesNameBase.FileData
}
