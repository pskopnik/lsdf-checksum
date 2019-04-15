package medasync

import (
	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

// truncateInsertsQuery is the SQL query performing an TRUNCATE TABLE
// statement on the inserts table. The query is meant to be run using exec and
// has no output.
//
// The query requires no parameters.
const truncateInsertsQuery = meda.GenericQuery(`
	TRUNCATE TABLE {INSERTS};
`)

// nextInsertsChunkQuery is the SQL query searching for the last row to be
// included in the next fixed-size chunk of rows of the inserts table. Each
// chunk consists of a number of rows whose IDs are in between two values.
// This allows chunked execution of queries on the inserts table. The query is
// meant to be run using query.
//
// The query requires several parameters:
//
//     previous_id - The ID previously returned by the query, i.e. the last ID
//                   of the previous chunk. For the first query, this should be
//                   set to 0.
//     chunk_size  - Number of records to be included in the chunk.
//
// The query outputs zero or a single row with one column. After the query has
// returned the last chunk, subsequent queries (with previous_id = last row ID
// of last chunk) return no rows.
//
//     id - The ID of the last row included in the chunk.
const nextInsertsChunkQuery = meda.GenericQuery(`
	(
		SELECT
			id
		FROM {INSERTS}
			WHERE id > ?
			ORDER BY id ASC
			LIMIT ?
	)
		ORDER BY id DESC LIMIT 1;
`)

// nextFilesChunkQuery is the SQL query searching for the last row to be
// included in the next fixed-size chunk of rows of the files table. Each
// chunk consists of a number of rows whose IDs are in between two values.
// This allows chunked execution of queries on the files table. The query is
// meant to be run using query.
//
// The query requires several parameters:
//
//     previous_id - The ID previously returned by the query, i.e. the last ID
//                   of the previous chunk. For the first query, this should be
//                   set to 0.
//     chunk_size  - Number of records to be included in the chunk.
//
// The query outputs zero or a single row with one column. After the query has
// returned the last chunk, subsequent queries (with previous_id = last row ID
// of last chunk) return no rows.
//
//     id - The ID of the last row included in the chunk.
const nextFilesChunkQuery = meda.GenericQuery(`
	(
		SELECT
			id
		FROM {FILES}
			WHERE id > ?
			ORDER BY id ASC
			LIMIT ?
	)
		ORDER BY id DESC LIMIT 1;
`)

// updateQuery is the SQL query updating files in the files table with new data
// written into the inserts table. The query is chunked on the inserts table
// (see nextInsertsChunkQuery). The query is meant to be run using exec and has
// no output.
//
// The query requires several parameters:
//
//     run_id           - The ID of the run for which the synchronisation
//                        takes place.
//     incremental_mode - 1 if the synchronisation takes place in incremental
//                        mode, 0 otherwise.
//     incremental_mode - 1 if the synchronisation takes place in incremental
//                        mode, 0 otherwise.
//     run_id           - The ID of the run for which the synchronisation
//                        takes place.
//     prev_inserts_id  - The last ID of the inserts table included in the
//                        previous chunk.
//     last_inserts_id  - The last ID of the inserts table included in the
//                        current chunk.
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
			ON {INSERTS}.path = {FILES}.path
		SET
			{FILES}.file_size = {INSERTS}.file_size,
			{FILES}.modification_time = {INSERTS}.modification_time,
			{FILES}.last_seen = ?,
			{FILES}.to_be_read = IF(?,
					-- incremental mode
					IF(to_be_read AND !to_be_compared,
							-- special case: file modified in aborted run
							1
						,
							-- only set to_be_read if the file was modified
							{FILES}.modification_time != {INSERTS}.modification_time
					)
				,
					-- full mode
					1
			),
			{FILES}.to_be_compared = IF(?,
					-- incremental mode
					0
				,
					-- full mode
					IF(to_be_read AND !to_be_compared,
							-- special case: file modified in aborted run
							0
						,
							-- only set to_be_compared if the file was not modified
							{FILES}.modification_time = {INSERTS}.modification_time
					)
			)
		WHERE
				{FILES}.last_seen != ?
			AND -- only process chunk
				{INSERTS}.id > ?
			AND
				{INSERTS}.id <= ?
	;
`)

// insertQuery is the SQL query inserting new files from the inserts table into
// the files table. The query is chunked on the inserts table (see
// nextInsertsChunkQuery). The query is meant to be run using exec and has no
// output.
//
// The query requires one parameter:
//
//     run_id          - The ID of the run for which the synchronisation takes
//                       place.
//     prev_inserts_id - The last ID of the inserts table included in the
//                       previous chunk.
//     last_inserts_id - The last ID of the inserts table included in the
//                        current chunk.
//
// After execution, the number of inserted / copied rows can be retrieved
// using RowsAffected().
const insertQuery = meda.GenericQuery(`
	INSERT INTO {FILES} (rand, path, file_size, modification_time, last_seen)
		SELECT RAND(), {INSERTS}.path, {INSERTS}.file_size, {INSERTS}.modification_time, ?
		FROM {INSERTS}
		LEFT JOIN {FILES} ON {INSERTS}.path = {FILES}.path
		WHERE
				{FILES}.id IS NULL
			AND -- only process chunk
				{INSERTS}.id > ?
			AND
				{INSERTS}.id <= ?
	;
`)

// deleteQuery is the SQL query deleting old files, i.e. files which no longer
// exist in the file system, from the files table. The query is chunked on the
// files table (see nextFilesChunkQuery). The query is meant to be run using
// exec and has no output.
//
// The query requires one parameter:
//
//     run_id        - The ID of the run for which the synchronisation takes
//                     place.
//     prev_files_id - The last ID of the files table included in the previous
//                     chunk.
//     last_files_id - The last ID of the files table included in the current
//                     chunk.
//
// After execution, the number of deleted rows can be retrieved using
// RowsAffected().
const deleteQuery = meda.GenericQuery(`
	DELETE FROM {FILES}
		WHERE
				last_seen != ?
			AND -- only process chunk
				id > ?
			AND
				id <= ?
	;
`)
