package meda

import (
	"context"

	"github.com/jmoiron/sqlx"
)

const ChecksumWarningsTableName = "checksum_warnings"

type ChecksumWarning struct {
	Id               uint64 `db:"id"`
	FileId           uint64 `db:"file_id"`
	Path             string `db:"path"`
	ModificationTime Time   `db:"modification_time"`
	FileSize         uint64 `db:"file_size"`
	ExpectedChecksum []byte `db:"expected_checksum"`
	ActualChecksum   []byte `db:"actual_checksum"`
	Discovered       uint64 `db:"discovered"`
	LastRead         uint64 `db:"last_read"`
	Created          Time   `db:"created"`
}

func ChecksumWarningsPrepareInsert(ctx context.Context, preparer NamedPreparerContext) (*sqlx.NamedStmt, error) {
	return preparer.PrepareNamedContext(ctx, "INSERT INTO checksum_warnings (file_id, path, modification_time, file_size, expected_checksum, actual_checksum, discovered, last_read, created) VALUES (:file_id, :path, :modification_time, :file_size, :expected_checksum, :actual_checksum, :discovered, :last_read, :created);")
}
