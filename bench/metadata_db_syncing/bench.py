#!/usr/bin/env python3

import mysql.connector
import datetime
import time
from argh import ArghParser


DROP_STATEMENT = "DROP TABLE IF EXISTS {};"

CREATE_STATEMENT = (
	"CREATE TABLE IF NOT EXISTS `files` ("
	"	`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,"
	"	`rand` double NOT NULL,"
	"	`path` varchar(4096) NOT NULL,"
	"	`modification_time` datetime(6) NOT NULL,"
	"	`file_size` bigint(20) unsigned NOT NULL,"
	"	`last_seen` bigint(20) unsigned NOT NULL,"
	"	`to_be_read` tinyint(3) unsigned NOT NULL DEFAULT 1,"
	"	`checksum` varbinary(256) DEFAULT NULL,"
	"	`last_read` bigint(20) unsigned DEFAULT NULL,"
	"	PRIMARY KEY (`id`),"
	"	KEY `rand` (`rand`),"
	"	KEY `path` (`path`(1024))"
	") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
)

INSERT_JOIN_STATEMENT = (
	"INSERT INTO files (rand, path, file_size, modification_time, last_seen)"
	"	SELECT {src}.rand, {src}.path, {src}.file_size, {src}.modification_time, {src}.last_seen"
	"	FROM {src}"
	"	LEFT JOIN files ON {src}.path = files.path"
	"	WHERE files.id IS NULL"
	"		AND"
	"			{src}.last_seen = %s"
	";"
)

INSERT_IN_STATEMENT = (
	"INSERT INTO files (rand, path, file_size, modification_time, last_seen)"
	"	SELECT rand, path, file_size, modification_time, last_seen"
	"	FROM {src}"
	"	WHERE {src}.path NOT IN"
	"			(SELECT DISTINCT path FROM files)"
	"		AND"
	"			last_seen = %s"
	";"
)

def drop_table(cnx, cursor, table):
	cursor.execute(DROP_STATEMENT.format(table))
	cnx.commit()

def reset(cnx, cursor):
	drop_table(cnx, cursor, "files")

	cursor.execute(CREATE_STATEMENT)
	cnx.commit()

def insert_join(cnx, cursor, src="inserts"):
	cursor.execute(INSERT_JOIN_STATEMENT.format(src=src), (1,))
	cnx.commit()

def insert_in(cnx, cursor, src="inserts"):
	cursor.execute(INSERT_IN_STATEMENT.format(src=src), (1,))
	cnx.commit()

def bench_empty():
	cnx = mysql.connector.connect(user='root', database='test_schema')
	cursor = cnx.cursor()

	join_times = list()

	for _ in range(10):
		reset(cnx, cursor)

		start = datetime.datetime.now()

		insert_join(cnx, cursor)

		end = datetime.datetime.now()

		join_times.append((end - start).total_seconds())

		time.sleep(60 * 5)

	print("join_times", join_times)

	in_times = list()

	for _ in range(10):
		reset(cnx, cursor)

		start = datetime.datetime.now()

		insert_in(cnx, cursor)

		end = datetime.datetime.now()

		in_times.append((end - start).total_seconds())

		time.sleep(60 * 5)

	print("in_times", in_times)

	cnx.commit()

	cursor.close()
	cnx.close()


CREATE_INSERTS_ORIGINAL_STATEMENT = (
	"CREATE TABLE IF NOT EXISTS `inserts_original` ("
	"	`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,"
	"	`rand` double NOT NULL,"
	"	`path` varchar(4096) NOT NULL,"
	"	`modification_time` datetime(6) NOT NULL,"
	"	`file_size` bigint(20) unsigned NOT NULL,"
	"	`last_seen` bigint(20) unsigned NOT NULL,"
	"	PRIMARY KEY (`id`)"
	") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
)

COPY_STATEMENT = (
	"INSERT INTO inserts_original (rand, path, file_size, modification_time, last_seen)"
	"	SELECT rand, path, file_size, modification_time, last_seen"
	"	FROM inserts"
	"	WHERE inserts.last_seen = %s"
	";"
)

UPDATE_INSERTS_STATEMENT = (
	"UPDATE inserts"
	"	SET last_seen = %s"
	"	WHERE last_seen = %s"
	";"
)


UPDATE_FILES_STATEMENT = (
	"UPDATE files"
	"	RIGHT JOIN inserts"
	"		ON inserts.path = files.path AND inserts.last_seen = %s"
	"	SET"
	"		files.rand = inserts.rand,"
	"		files.file_size = inserts.file_size,"
	"		files.modification_time = inserts.modification_time,"
	"		files.last_seen = inserts.last_seen,"
	"		files.to_be_read = IF(files.modification_time = inserts.modification_time, 0, 1)"
	";"
)

DELETE_FILES_STATEMENT = (
	"DELETE FROM files"
	"	WHERE last_seen != %s"
	";"
)

def prepare_inserts(cnx, cursor):
	cursor.execute(CREATE_INSERTS_ORIGINAL_STATEMENT)

	cursor.execute(COPY_STATEMENT, (1,))

	cursor.execute(UPDATE_INSERTS_STATEMENT, (2, 1))

	cnx.commit()

def restore_inserts(cnx, cursor):
	drop_table(cnx, cursor, "inserts_original")

	cursor.execute(UPDATE_INSERTS_STATEMENT, (1, 2))

	cnx.commit()

def update_files(cnx, cursor):
	cursor.execute(UPDATE_FILES_STATEMENT, (2,))

	cnx.commit()

def delete_files(cnx, cursor):
	cursor.execute(DELETE_FILES_STATEMENT, (2,))

	cnx.commit()

def bench_no_inserts():
	cnx = mysql.connector.connect(user='root', database='test_schema')
	cursor = cnx.cursor()

	prepare_inserts(cnx, cursor)

	update_times = list()
	delete_times = list()

	join_times = list()

	for _ in range(10):
		reset(cnx, cursor)
		insert_join(cnx, cursor, src="inserts_original")

		time.sleep(60 * 5)

		start = datetime.datetime.now()
		update_files(cnx, cursor)
		end = datetime.datetime.now()

		update_times.append((end - start).total_seconds())

		start = datetime.datetime.now()
		insert_join(cnx, cursor)
		end = datetime.datetime.now()

		join_times.append((end - start).total_seconds())

		start = datetime.datetime.now()
		delete_files(cnx, cursor)
		end = datetime.datetime.now()

		delete_times.append((end - start).total_seconds())

		time.sleep(60 * 5)

	print("join_times", join_times)

	in_times = list()

	for _ in range(10):
		reset(cnx, cursor)
		insert_join(cnx, cursor, src="inserts_original")

		time.sleep(60 * 5)

		start = datetime.datetime.now()
		update_files(cnx, cursor)
		end = datetime.datetime.now()

		update_times.append((end - start).total_seconds())

		start = datetime.datetime.now()
		insert_in(cnx, cursor)
		end = datetime.datetime.now()

		in_times.append((end - start).total_seconds())

		start = datetime.datetime.now()
		delete_files(cnx, cursor)
		end = datetime.datetime.now()

		delete_times.append((end - start).total_seconds())

		time.sleep(60 * 5)

	print("in_times", in_times)

	print("update_times", update_times)

	print("delete_times", delete_times)

	restore_inserts(cnx, cursor)

	cnx.commit()

	cursor.close()
	cnx.close()


parser = ArghParser()
parser.add_commands([bench_empty, bench_no_inserts])

def main():
	parser.dispatch()

if __name__ == '__main__':
	main()
