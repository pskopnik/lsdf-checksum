package medasync

import (
	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

// cleanInsertsQuery is the SQL query removing files read during a specific
// run from the inserts table. The query is meant to be run using exec and has
// no output.
//
// The query requires one parameter:
//
//     run_id - The Id of the run for which the synchronisation takes place.
//
// After execution, the number of deleted rows can be retrieved using
// RowsAffected().
const cleanInsertsQuery = meda.GenericQuery(`
	DELETE FROM {INSERTS}
		WHERE last_seen = ?
	;
`)

// lockInsertsQuery is the SQL query acquiring an exclusive write lock on the
// inserts table for the transaction it is executed in. The query is meant to
// be run using exec and has no output. This query should only be used as part
// of a transaction due to the way database/sql handles connection pooling.
//
//     http://go-database-sql.org/surprises.html#connection-state-mismatch
//
// The query requires no parameters.
const lockInsertsQuery = meda.GenericQuery(`
	LOCK TABLES
		{INSERTS} WRITE
	;
`)

// unlockQuery is the SQL query releasing all locks acquired by the
// transaction it is executed in. The query is meant to be run using exec and
// has no output. This query should only be used as part of the transaction
// SQL LOCK statements are run in due to the way database/sql handles
// connection pooling.
//
//     http://go-database-sql.org/surprises.html#connection-state-mismatch
//
// The query requires no parameters.
const unlockQuery = meda.GenericQuery(`
	UNLOCK TABLES;
`)

// optimiseInsertsQuery is the SQL query performing an OPTIMIZE TABLE
// statement on the inserts table. The query is meant to be run using query.
//
// The query requires no parameters.
//
// The query returns the complete output of the OPTIMIZE TABLE statement.
//
//     https://dev.mysql.com/doc/refman/8.0/en/optimize-table.html
//     https://mariadb.com/kb/en/library/optimize-table/
const optimiseInsertsQuery = meda.GenericQuery(`
	OPTIMIZE TABLE {INSERTS};
`)

// rowExistsInsertsQuery is the SQL query returning whether there are any rows
// in the inserts table. The query is meant to be run using query.
//
// The query requires no parameters.
//
// The query outputs a single row with one column:
//
//     row_exists - 1 if at least one row exists in the inserts table, 0
//                  otherwise.
const rowExistsInsertsQuery = meda.GenericQuery(`
	SELECT
		EXISTS (SELECT 1 FROM {INSERTS}) AS row_exists
	;
`)

// updateQuery is the SQL query updating files in the files table with new
// data written into the inserts table. The query is meant to be run using
// exec and has no output.
//
// The query requires several parameters:
//
//     run_id           - The Id of the run for which the synchronisation
//                        takes place.
//     incremental_mode - 1 if the synchronisation takes place in incremental
//                        mode, 0 otherwise.
//     incremental_mode - 1 if the synchronisation takes place in incremental
//                        mode, 0 otherwise.
//     run_id           - The Id of the run for which the synchronisation
//                        takes place.
//
// The to_be_read field is set to 1 if not running in incremental mode. In
// incremental mode the field is set to 1 if the modification_time differs.
//
// The to_be_compared field is set to 0 if running in incremental model.
// Otherwise the field is set to 0 if the modification_time does not differ.
//
// After execution, the number of updated rows can be retrieved using
// RowsAffected().
const updateQuery = meda.GenericQuery(`
	UPDATE {FILES}
		RIGHT JOIN {INSERTS}
			ON {INSERTS}.path = {FILES}.path AND {INSERTS}.last_seen = ?
		SET
			{FILES}.rand = RAND(),
			{FILES}.file_size = {INSERTS}.file_size,
			{FILES}.modification_time = {INSERTS}.modification_time,
			{FILES}.last_seen = {INSERTS}.last_seen,
			{FILES}.to_be_read = IF(?, IF({FILES}.modification_time = {INSERTS}.modification_time, 0, 1), 1),
			{FILES}.to_be_compared = IF(?, 0, IF({FILES}.modification_time = {INSERTS}.modification_time, 1, 0))
		WHERE {FILES}.last_seen != ?
	;
`)

// insertQuery is the SQL query inserting new files from the inserts table
// into the files table. The query is meant to be run using exec and has no
// output.
//
// The query requires one parameter:
//
//     run_id - The Id of the run for which the synchronisation takes place.
//
// After execution, the number of inserted / copied rows can be retrieved
// using RowsAffected().
const insertQuery = meda.GenericQuery(`
	INSERT INTO {FILES} (rand, path, file_size, modification_time, last_seen)
		SELECT RAND(), {INSERTS}.path, {INSERTS}.file_size, {INSERTS}.modification_time, {INSERTS}.last_seen
		FROM {INSERTS}
		LEFT JOIN {FILES} ON {INSERTS}.path = {FILES}.path
		WHERE {FILES}.id IS NULL
			AND
				{INSERTS}.last_seen = ?
	;
`)

// deleteQuery is the SQL query deleting old files, i.e. files which no longer
// exist in the file system, from the files table. The query is meant to be
// run using exec and has no output.
//
// The query requires one parameter:
//
//     run_id - The Id of the run for which the synchronisation takes place.
//
// After execution, the number of deleted rows can be retrieved using
// RowsAffected().
const deleteQuery = meda.GenericQuery(`
	DELETE FROM {FILES}
		WHERE last_seen != ?
	;
`)
