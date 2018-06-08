package meda

import (
	"time"

	"github.com/jmoiron/sqlx"
)

const InsertsTableName = "inserts"

type Insert struct {
	Id               int       `db:"id"`
	Path             string    `db:"path"`
	ModificationTime time.Time `db:"modification_time"`
	FileSize         int       `db:"file_size"`
	LastSeen         int       `db:"last_seen"`
}

func InsertsPrepareInsert(preparer NamedPreparer) (*sqlx.NamedStmt, error) {
	return preparer.PrepareNamed("INSERT INTO inserts (path, modification_time, file_size, last_seen) VALUES (:path, :modification_time, :file_size, :last_seen)")
}
