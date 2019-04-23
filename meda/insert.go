package meda

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

const insertsTableNameBase = "inserts"

func (d *DB) InsertsTableName() string {
	return d.Config.TablePrefix + insertsTableNameBase
}

const insertsCreateTableQuery = GenericQuery(`
	CREATE TABLE IF NOT EXISTS {INSERTS} (
		id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
		path varbinary(4096) NOT NULL,
		modification_time datetime(6) NOT NULL,
		file_size bigint(20) unsigned NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`)

func (d *DB) insertsCreateTable(ctx context.Context) error {
	_, err := d.ExecContext(ctx, insertsCreateTableQuery.SubstituteAll(d))
	if err != nil {
		return errors.Wrap(err, "(*DB).insertsCreateTable")
	}

	return nil
}

type Insert struct {
	ID               uint64 `db:"id"`
	Path             string `db:"path"`
	ModificationTime Time   `db:"modification_time"`
	FileSize         uint64 `db:"file_size"`
}

const insertsPrepareInsertQuery = GenericQuery(`
	INSERT INTO {INSERTS} (
			path, modification_time, file_size
		) VALUES (
			:path, :modification_time, :file_size
		)
	;
`)

func (d *DB) InsertsPrepareInsert(ctx context.Context, preparer NamedPreparerContext) (*sqlx.NamedStmt, error) {
	if preparer == nil {
		preparer = &d.DB
	}

	stmt, err := preparer.PrepareNamedContext(ctx, insertsPrepareInsertQuery.SubstituteAll(d))
	if err != nil {
		return nil, errors.Wrap(err, "(*DB).InsertsPrepareInsert")
	}

	return stmt, nil
}
