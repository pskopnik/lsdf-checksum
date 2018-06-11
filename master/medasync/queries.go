package medasync

import (
	"strings"

	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

type substitutedQuery string

func (s substitutedQuery) get() string {
	return strings.NewReplacer(
		"{FILES}", meda.FilesTableName,
		"{INSERTS}", meda.InsertsTableName,
	).Replace(string(s))
}

var updateQuery = substitutedQuery(
	`UPDATE {FILES}
		RIGHT JOIN {INSERTS}
			ON {INSERTS}.path = {FILES}.path AND {INSERTS}.last_seen = ?
		SET
			{FILES}.file_size = {INSERTS}.file_size,
			{FILES}.modification_time = {INSERTS}.modification_time,
			{FILES}.last_seen = {INSERTS}.last_seen,
			{FILES}.to_be_read = IF({FILES}.modification_time = {INSERTS}.modification_time, 0, 1)
		WHERE {FILES}.last_seen != ?
	;`,
)

var insertQuery = substitutedQuery(
	`INSERT INTO {FILES} (path, file_size, modification_time, last_seen)
		SELECT {INSERTS}.path, {INSERTS}.file_size, {INSERTS}.modification_time, {INSERTS}.last_seen
		FROM {INSERTS}
		LEFT JOIN {FILES} ON {INSERTS}.path = {FILES}.path
		WHERE {FILES}.id IS NULL
			AND
				{INSERTS}.last_seen = ?
	;`,
)

var deleteQuery = substitutedQuery(
	`DELETE FROM {FILES}
		WHERE last_seen != ?
	;`,
)

var cleanInsertsQuery = substitutedQuery(
	`DELETE FROM {INSERTS}
		WHERE last_seen = ?
	;`,
)
