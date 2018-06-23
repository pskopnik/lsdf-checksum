package meda

import (
	"time"

	_ "github.com/jmoiron/sqlx"
)

const FilesTableName = "files"

type File struct {
	Id               int       `db:"id"`
	Rand             float64   `db:"rand"`
	Path             string    `db:"path"`
	ModificationTime time.Time `db:"modification_time"`
	FileSize         int       `db:"file_size"`
	LastSeen         int       `db:"last_seen"`
	ToBeRead         int       `db:"to_be_read"`
	Checksum         []byte    `db:"checksum"`
	LastRead         int       `db:"last_read"`
}
