package meda

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
)

const ChecksumWarningsTableName = "checksum_warnings"

type ChecksumWarning struct {
	Id               int       `db:"id"`
	FileId           int       `db:"file_id"`
	Path             string    `db:"path"`
	ModificationTime time.Time `db:"modification_time"`
	FileSize         int       `db:"file_size"`
	ExpectedChecksum []byte    `db:"expected_checksum"`
	ActualChecksum   []byte    `db:"actual_checksum"`
	Discovered       int       `db:"discovered"`
	LastRead         int       `db:"last_read"`
	Created          time.Time `db:"created"`
}

func ChecksumWarningsPrepareInsert(ctx context.Context, preparer NamedPreparerContext) (*sqlx.NamedStmt, error) {
	return preparer.PrepareNamedContext(ctx, "INSERT INTO checksum_warnings (file_id, path, modification_time, expected_checksum, actual_checksum, discovered, last_read, created) VALUES (:file_id, :path, :modification_time, :expected_checksum, :actual_checksum, :discovered, :last_read, :created);")
}
