package meda

import (
	"context"

	"github.com/jmoiron/sqlx"
)

const checksumWarningsTableNameBase = "checksum_warnings"

func (d *DB) ChecksumWarningsTableName() string {
	return d.Config.TablePrefix + checksumWarningsTableNameBase
}

const checksumWarningsCreateTableQuery = GenericQuery(`
	CREATE TABLE IF NOT EXISTS {CHECKSUM_WARNINGS} (
		id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
		file_id bigint(20) unsigned NOT NULL,
		path varbinary(4096) NOT NULL,
		modification_time datetime(6) NOT NULL,
		file_size bigint(20) unsigned NOT NULL,
		expected_checksum varbinary(64) NOT NULL,
		actual_checksum varbinary(64) NOT NULL,
		discovered bigint(20) unsigned NOT NULL,
		last_read bigint(20) unsigned NOT NULL,
		created datetime(6) NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`)

func (d *DB) checksumWarningsCreateTable(ctx context.Context) error {
	_, err := d.ExecContext(ctx, checksumWarningsCreateTableQuery.SubstituteAll(d))
	return err
}

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

const checksumWarningsPrepareInsertQuery = GenericQuery(`
	INSERT INTO {CHECKSUM_WARNINGS} (
			file_id,
			path,
			modification_time,
			file_size,
			expected_checksum,
			actual_checksum,
			discovered,
			last_read,
			created
		) VALUES (
			:file_id,
			:path,
			:modification_time,
			:file_size,
			:expected_checksum,
			:actual_checksum,
			:discovered,
			:last_read,
			:created
		)
	;
`)

func (d *DB) ChecksumWarningsPrepareInsert(ctx context.Context, preparer NamedPreparerContext) (*sqlx.NamedStmt, error) {
	if preparer == nil {
		preparer = &d.DB
	}

	return preparer.PrepareNamedContext(ctx, checksumWarningsPrepareInsertQuery.SubstituteAll(d))
}
