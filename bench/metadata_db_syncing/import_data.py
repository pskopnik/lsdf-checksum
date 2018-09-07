#!/usr/bin/env python3

import mysql.connector
import re
from argh import ArghParser


DROP_TABLE_IF_EXISTS_STMT = (
	"DROP TABLE IF EXISTS {name};"
)

CREATE_FILE_DATA_TABLE_STMT = (
	"CREATE TABLE IF NOT EXISTS `{name}` ("
	"	`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,"
	"	`path` varbinary(4096) NOT NULL,"
	"	`modification_time` datetime(6) NOT NULL,"
	"	`file_size` bigint(20) unsigned NOT NULL,"
	"	PRIMARY KEY (`id`)"
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
)

INSERT_FILE_DATA_STMT = (
	"INSERT INTO {name} ("
	"		path, modification_time, file_size"
	"	) VALUES ("
	"		%s, %s, %s"
	"	)"
	";"
)

LINE_RE = re.compile("^\\d+ +\\d+ +\\d+ +\\|(\\d+)\\|([\\w-]+ [\\w:.]+)\\| +-- +(.+)$")


def read_generator(path):
	with open(path) as f:
		for line in f.readlines():
			match = LINE_RE.match(line)
			if match is not None:
				yield (
					match.group(3),
					match.group(2),
					int(match.group(1))
				)


def import_data(path, table_name="file_data", recreate=True):
	cnx = mysql.connector.connect(user='root', database='test_schema')
	cursor = cnx.cursor()

	if recreate:
		cursor.execute(DROP_TABLE_IF_EXISTS_STMT.format(name=table_name))
		cnx.commit()

	cursor.execute(CREATE_FILE_DATA_TABLE_STMT.format(name=table_name))
	cnx.commit()

	insert_file_data_stmt = INSERT_FILE_DATA_STMT.format(name=table_name)

	i = 0

	for el in read_generator(path):
		cursor.execute(insert_file_data_stmt, el)

		i += 1
		if i == 10000:
			cnx.commit()
			i = 0

	cnx.commit()

	cursor.close()
	cnx.close()


parser = ArghParser()
parser.set_default_command(import_data)

def main():
	parser.dispatch()

if __name__ == '__main__':
	main()
