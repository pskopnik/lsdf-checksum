package meda

import (
	"context"

	"github.com/jmoiron/sqlx"
)

const insertsTableNameBase = "inserts"

func (d *DB) InsertsTableName() string {
	return d.Config.TablePrefix + insertsTableNameBase
}

const insertsCreateTableQuery = GenericQuery(`
	CREATE TABLE IF NOT EXISTS {INSERTS} (
		id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
		rand double NOT NULL,
		path varbinary(4096) NOT NULL,
		modification_time datetime(6) NOT NULL,
		file_size bigint(20) unsigned NOT NULL,
		last_seen bigint(20) unsigned NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`)

func (d *DB) insertsCreateTable(ctx context.Context) error {
	_, err := d.ExecContext(ctx, insertsCreateTableQuery.SubstituteAll(d))
	return err
}

type Insert struct {
	Id               uint64  `db:"id"`
	Rand             float64 `db:"rand"`
	Path             string  `db:"path"`
	ModificationTime Time    `db:"modification_time"`
	FileSize         uint64  `db:"file_size"`
	LastSeen         uint64  `db:"last_seen"`
}

const insertsPrepareInsertQuery = GenericQuery(`
	INSERT INTO {INSERTS} (
			rand, path, modification_time, file_size, last_seen
		) VALUES (
			RAND(), :path, :modification_time, :file_size, :last_seen
		)
	;
`)

func (d *DB) InsertsPrepareInsert(ctx context.Context, preparer NamedPreparerContext) (*sqlx.NamedStmt, error) {
	if preparer == nil {
		preparer = &d.DB
	}

	return preparer.PrepareNamedContext(ctx, insertsPrepareInsertQuery.SubstituteAll(d))
}
