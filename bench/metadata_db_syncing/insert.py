#!/usr/bin/env python3

import mysql.connector
import re
from argh import ArghParser


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

def insert(path, last_seen=1):
	cnx = mysql.connector.connect(user='root', database='test_schema')
	cursor = cnx.cursor()

	add_insert = (
		"INSERT INTO inserts "
		"(path, modification_time, file_size, last_seen) "
		"VALUES (%s, %s, %s, %s)"
	)

	for el in read_generator(path):
		cursor.execute(add_insert, el + (last_seen,))

	cnx.commit()

	cursor.close()
	cnx.close()

parser = ArghParser()
parser.set_default_command(insert)

def main():
	parser.dispatch()

if __name__ == '__main__':
	main()
