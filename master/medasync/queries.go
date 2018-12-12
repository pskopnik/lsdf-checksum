package medasync

import (
	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

const cleanInsertsQuery = meda.GenericQuery(`
	DELETE FROM {INSERTS}
		WHERE last_seen = ?
	;
`)

const lockInsertsQuery = meda.GenericQuery(`
	LOCK TABLES
		{INSERTS} WRITE
	;
`)

const unlockQuery = meda.GenericQuery(`
	UNLOCK TABLES;
`)

const optimiseInsertsQuery = meda.GenericQuery(`
	OPTIMIZE TABLE {INSERTS};
`)

const rowExistsInsertsQuery = meda.GenericQuery(`
	SELECT
		EXISTS (SELECT 1 FROM {INSERTS}) AS row_exists
	;
`)

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

const deleteQuery = meda.GenericQuery(`
	DELETE FROM {FILES}
		WHERE last_seen != ?
	;
`)
