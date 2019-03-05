#!/usr/bin/env python3

import csv
import datetime
import sys

import mysql.connector
from argh import ArghParser


class TestCase(object):
	def create_inserts_table(self, cnx):
		raise NotImplemented

	def create_files_table(self, cnx):
		raise NotImplemented

	def fast_populate_inserts(self, cnx, src_table):
		"""Populates the inserts table with data from src_table.

		src_table has the fields (path, modification_time, file_size).
		"""
		raise NotImplemented

	def fast_copy_to_files(self, cnx, src_table, run_id):
		raise NotImplemented

	def insert_new_files(self, cnx, run_id):
		raise NotImplemented

	def update_existing_files(self, cnx, run_id):
		raise NotImplemented

	def delete_old_files(self, cnx, run_id):
		raise NotImplemented


class TestCaseBase(TestCase):
	POPULATE_INSERTS_STMT = (
		"INSERT INTO `inserts`"
		"	(`path`, `modification_time`, `file_size`)"
		"		SELECT"
		"			`path`, `modification_time`, `file_size`"
		"		FROM `{src}`"
		";"
	)

	CREATE_INSERTS_TABLE_STMT = (
		"CREATE TABLE `inserts` ("
		"	`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,"
		"	`path` varbinary(4096) NOT NULL,"
		"	`modification_time` datetime(6) NOT NULL,"
		"	`file_size` bigint(20) unsigned NOT NULL,"
		"	PRIMARY KEY (`id`)"
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
	)

	CREATE_FILES_TABLE_STMT = (
		"CREATE TABLE `files` ("
		"	`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,"
		"	`rand` double NOT NULL,"
		"	`path` varbinary(4096) NOT NULL,"
		"	`modification_time` datetime(6) NOT NULL,"
		"	`file_size` bigint(20) unsigned NOT NULL,"
		"	`last_seen` bigint(20) unsigned NOT NULL,"
		"	`to_be_read` tinyint(3) unsigned NOT NULL DEFAULT 1,"
		"   `to_be_compared` tinyint(3) unsigned NOT NULL DEFAULT 0,"
		"	`checksum` varbinary(256) DEFAULT NULL,"
		"	`last_read` bigint(20) unsigned DEFAULT NULL,"
		"	PRIMARY KEY (`id`),"
		"	KEY `rand` (`rand`),"
		"	KEY `path` (`path`(2048))"
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
	)

	UPDATE_EXISTING_FILES_STMT = None
	INSERT_NEW_FILES_STMT = None
	DELETE_OLD_FILES_STMT = None

	def create_inserts_table(self, cnx):
		self._execute_stmt(cnx, self.CREATE_INSERTS_TABLE_STMT)

	def create_files_table(self, cnx):
		self._execute_stmt(cnx, self.CREATE_FILES_TABLE_STMT)

	def fast_populate_inserts(self, cnx, src_table):
		stmt = self.POPULATE_INSERTS_STMT.format(src=src_table)

		self._execute_stmt(cnx, stmt)

	def fast_copy_to_files(self, cnx, src_table, run_id):
		return self.insert_new_files(cnx, run_id, src_table=src_table)

	def insert_new_files(self, cnx, run_id, src_table="inserts"):
		stmt = self.INSERT_NEW_FILES_STMT.format(src=src_table, run=run_id)

		return self._analyze_stmt(cnx, stmt, isolation_level='READ COMMITTED')

	def update_existing_files(self, cnx, run_id):
		stmt = self.UPDATE_EXISTING_FILES_STMT.format(run=run_id)

		return self._analyze_stmt(cnx, stmt, isolation_level='READ COMMITTED')

	def delete_old_files(self, cnx, run_id):
		stmt = self.DELETE_OLD_FILES_STMT.format(run=run_id)

		return self._analyze_stmt(cnx, stmt, isolation_level='READ COMMITTED')

	def _execute_stmt(self, cnx, stmt, isolation_level=None):
		cnx.start_transaction(isolation_level=isolation_level)
		cursor = cnx.cursor()

		cursor.execute(stmt)
		result = list(cursor)

		cnx.commit()
		cursor.close()

		return result

	def _analyze_stmt(self, cnx, stmt, isolation_level=None):
		stmt = "ANALYZE FORMAT=json " + stmt

		cnx.start_transaction(isolation_level=isolation_level)
		cursor = cnx.cursor()

		cursor.execute(stmt)
		results = list(cursor)

		cnx.commit()
		cursor.close()

		if len(results) > 0 and len(results[0]) > 0:
			return results[0][0]
		else:
			return None


class TestCaseBaseNoRand(TestCaseBase):
	POPULATE_INSERTS_STMT = (
		"INSERT INTO `inserts`"
		"	(`path`, `modification_time`, `file_size`)"
		"		SELECT"
		"			`path`, `modification_time`, `file_size`"
		"		FROM `{src}`"
		";"
	)

	CREATE_INSERTS_TABLE_STMT = (
		"CREATE TABLE `inserts` ("
		"	`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,"
		"	`path` varbinary(4096) NOT NULL,"
		"	`modification_time` datetime(6) NOT NULL,"
		"	`file_size` bigint(20) unsigned NOT NULL,"
		"	PRIMARY KEY (`id`)"
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
	)

	CREATE_FILES_TABLE_STMT = (
		"CREATE TABLE `files` ("
		"	`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,"
		"	`path` varbinary(4096) NOT NULL,"
		"	`modification_time` datetime(6) NOT NULL,"
		"	`file_size` bigint(20) unsigned NOT NULL,"
		"	`last_seen` bigint(20) unsigned NOT NULL,"
		"	`to_be_read` tinyint(3) unsigned NOT NULL DEFAULT 1,"
		"   `to_be_compared` tinyint(3) unsigned NOT NULL DEFAULT 0,"
		"	`checksum` varbinary(256) DEFAULT NULL,"
		"	`last_read` bigint(20) unsigned DEFAULT NULL,"
		"	PRIMARY KEY (`id`),"
		"	KEY `path` (`path`(2048))"
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
	)


class TestCaseNoRand(TestCaseBaseNoRand):
	UPDATE_EXISTING_FILES_STMT = (
		"UPDATE files"
		"	RIGHT JOIN inserts"
		"		ON inserts.path = files.path"
		"	SET"
		"		files.file_size = inserts.file_size,"
		"		files.modification_time = inserts.modification_time,"
		"		files.last_seen = {run},"
		"		files.to_be_read = 1,"
		"		files.to_be_compared = IF(files.modification_time = inserts.modification_time, 1, 0)"
		"	WHERE files.last_seen != {run}"
		";"
	)

	INSERT_NEW_FILES_STMT = (
		"INSERT INTO files (path, file_size, modification_time, last_seen)"
		"	SELECT {src}.path, {src}.file_size, {src}.modification_time, {run}"
		"	FROM {src}"
		"	LEFT JOIN files ON {src}.path = files.path"
		"	WHERE files.id IS NULL"
		";"
	)

	DELETE_OLD_FILES_STMT = (
		"DELETE FROM files"
		"	WHERE last_seen != {run}"
		";"
	)


class TestCaseInitial(TestCaseBase):
	UPDATE_EXISTING_FILES_STMT = (
		"UPDATE files"
		"	RIGHT JOIN inserts"
		"		ON inserts.path = files.path"
		"	SET"
		"		files.rand = RAND(),"
		"		files.file_size = inserts.file_size,"
		"		files.modification_time = inserts.modification_time,"
		"		files.last_seen = {run},"
		"		files.to_be_read = 1,"
		"		files.to_be_compared = IF(files.modification_time = inserts.modification_time, 1, 0)"
		"	WHERE files.last_seen != {run}"
		";"
	)

	INSERT_NEW_FILES_STMT = (
		"INSERT INTO files (rand, path, file_size, modification_time, last_seen)"
		"	SELECT RAND(), {src}.path, {src}.file_size, {src}.modification_time, {run}"
		"	FROM {src}"
		"	LEFT JOIN files ON {src}.path = files.path"
		"	WHERE files.id IS NULL"
		";"
	)

	DELETE_OLD_FILES_STMT = (
		"DELETE FROM files"
		"	WHERE last_seen != {run}"
		";"
	)


class TestCaseInsertAllConditionsInOn(TestCaseInitial):
	INSERT_NEW_FILES_STMT = (
		"INSERT INTO files (rand, path, file_size, modification_time, last_seen)"
		"	SELECT RAND(), {src}.path, {src}.file_size, {src}.modification_time, {run}"
		"	FROM {src}"
		"	LEFT JOIN files"
		"		ON {src}.path = files.path AND files.id IS NULL"
		";"
	)


class TestCaseInsertExists(TestCaseInitial):
	INSERT_NEW_FILES_STMT = (
		"INSERT INTO files (rand, path, file_size, modification_time, last_seen)"
		"	SELECT RAND(), {src}.path, {src}.file_size, {src}.modification_time, {run}"
		"	FROM {src}"
		"	WHERE"
		"			NOT EXISTS ("
		"				SELECT 1 FROM files"
		"					WHERE files.path = {src}.path"
		"			)"
		";"
	)


class TestCaseUpdateAllConditionsInOn(TestCaseInitial):
	UPDATE_EXISTING_FILES_STMT = (
		"UPDATE files"
		"	RIGHT JOIN inserts"
		"		ON inserts.path = files.path AND files.last_seen != {run}"
		"	SET"
		"		files.rand = RAND(),"
		"		files.file_size = inserts.file_size,"
		"		files.modification_time = inserts.modification_time,"
		"		files.last_seen = {run},"
		"		files.to_be_read = 1,"
		"		files.to_be_compared = IF(files.modification_time = inserts.modification_time, 1, 0)"
		";"
	)


class TestCaseUpdateNoJoinConditions(TestCaseInitial):
	UPDATE_EXISTING_FILES_STMT = (
		"UPDATE files"
		"	RIGHT JOIN inserts"
		"		ON inserts.path = files.path"
		"	SET"
		"		files.rand = RAND(),"
		"		files.file_size = inserts.file_size,"
		"		files.modification_time = inserts.modification_time,"
		"		files.last_seen = {run},"
		"		files.to_be_read = 1,"
		"		files.to_be_compared = IF(files.modification_time = inserts.modification_time, 1, 0)"
		";"
	)


class TestCaseUpdateIndexInsertsPath(TestCaseUpdateNoJoinConditions):
	CREATE_INSERTS_TABLE_STMT = (
		"CREATE TABLE `inserts` ("
		"	`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,"
		"	`path` varbinary(4096) NOT NULL,"
		"	`modification_time` datetime(6) NOT NULL,"
		"	`file_size` bigint(20) unsigned NOT NULL,"
		"	PRIMARY KEY (`id`),"
		"	KEY `path` (`path`(2048))"
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
	)


class TestCaseUpdateNoInsertsConditions(TestCaseInitial):
	UPDATE_EXISTING_FILES_STMT = (
		"UPDATE files"
		"	RIGHT JOIN inserts"
		"		ON inserts.path = files.path"
		"	SET"
		"		files.rand = RAND(),"
		"		files.file_size = inserts.file_size,"
		"		files.modification_time = inserts.modification_time,"
		"		files.last_seen = {run},"
		"		files.to_be_read = 1,"
		"		files.to_be_compared = IF(files.modification_time = inserts.modification_time, 1, 0)"
		"	WHERE files.last_seen != {run}"
		";"
	)


class TestCaseUpdateNoInsertsConditionsAllInOn(TestCaseUpdateNoInsertsConditions):
	UPDATE_EXISTING_FILES_STMT = (
		"UPDATE files"
		"	RIGHT JOIN inserts"
		"		ON inserts.path = files.path AND files.last_seen != {run}"
		"	SET"
		"		files.rand = RAND(),"
		"		files.file_size = inserts.file_size,"
		"		files.modification_time = inserts.modification_time,"
		"		files.last_seen = {run},"
		"		files.to_be_read = 1,"
		"		files.to_be_compared = IF(files.modification_time = inserts.modification_time, 1, 0)"
		";"
	)


class TestCaseUpdateNoInsertsConditionsAllInOnCompositeIndex(TestCaseUpdateNoInsertsConditionsAllInOn):
	CREATE_FILES_TABLE_STMT = (
		"CREATE TABLE `files` ("
		"	`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,"
		"	`rand` double NOT NULL,"
		"	`path` varbinary(4096) NOT NULL,"
		"	`modification_time` datetime(6) NOT NULL,"
		"	`file_size` bigint(20) unsigned NOT NULL,"
		"	`last_seen` bigint(20) unsigned NOT NULL,"
		"	`to_be_read` tinyint(3) unsigned NOT NULL DEFAULT 1,"
		"   `to_be_compared` tinyint(3) unsigned NOT NULL DEFAULT 0,"
		"	`checksum` varbinary(256) DEFAULT NULL,"
		"	`last_read` bigint(20) unsigned DEFAULT NULL,"
		"	PRIMARY KEY (`id`),"
		"	KEY `rand` (`rand`),"
		"	KEY `path_last_seen` (`path`(2048),`last_seen`)"
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
	)


class TestCaseRandInInserts(TestCaseInitial):
	CREATE_INSERTS_TABLE_STMT = (
		"CREATE TABLE `inserts` ("
		"	`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,"
		"	`rand` double NOT NULL,"
		"	`path` varbinary(4096) NOT NULL,"
		"	`modification_time` datetime(6) NOT NULL,"
		"	`file_size` bigint(20) unsigned NOT NULL,"
		"	PRIMARY KEY (`id`)"
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
	)

	POPULATE_INSERTS_STMT = (
		"INSERT INTO `inserts`"
		"	(`rand`, `path`, `modification_time`, `file_size`)"
		"		SELECT"
		"			RAND(), `path`, `modification_time`, `file_size`"
		"		FROM `{src}`"
		";"
	)

	UPDATE_EXISTING_FILES_STMT = (
		"UPDATE files"
		"	RIGHT JOIN inserts"
		"		ON inserts.path = files.path"
		"	SET"
		"		files.rand = inserts.rand,"
		"		files.file_size = inserts.file_size,"
		"		files.modification_time = inserts.modification_time,"
		"		files.last_seen = {run},"
		"		files.to_be_read = 1,"
		"		files.to_be_compared = IF(files.modification_time = inserts.modification_time, 1, 0)"
		"	WHERE files.last_seen != {run}"
		";"
	)

	INSERT_NEW_FILES_STMT = (
		"INSERT INTO files (rand, path, file_size, modification_time, last_seen)"
		"	SELECT {src}.rand, {src}.path, {src}.file_size, {src}.modification_time, {run}"
		"	FROM {src}"
		"	LEFT JOIN files ON {src}.path = files.path"
		"	WHERE files.id IS NULL"
		";"
	)


class TestCaseDontUpdateRand(TestCaseInitial):
	UPDATE_EXISTING_FILES_STMT = (
		"UPDATE files"
		"	RIGHT JOIN inserts"
		"		ON inserts.path = files.path"
		"	SET"
		"		files.file_size = inserts.file_size,"
		"		files.modification_time = inserts.modification_time,"
		"		files.last_seen = {run},"
		"		files.to_be_read = 1,"
		"		files.to_be_compared = IF(files.modification_time = inserts.modification_time, 1, 0)"
		"	WHERE files.last_seen != {run}"
		";"
	)


class TestCaseDontUpdateRandUpdateAllConditionsInOn(TestCaseDontUpdateRand):
	UPDATE_EXISTING_FILES_STMT = (
		"UPDATE files"
		"	RIGHT JOIN inserts"
		"		ON inserts.path = files.path AND files.last_seen != {run}"
		"	SET"
		"		files.file_size = inserts.file_size,"
		"		files.modification_time = inserts.modification_time,"
		"		files.last_seen = {run},"
		"		files.to_be_read = 1,"
		"		files.to_be_compared = IF(files.modification_time = inserts.modification_time, 1, 0)"
		";"
	)


class TestCaseDontUpdateRandUpdateNoJoinConditions(TestCaseDontUpdateRand):
	UPDATE_EXISTING_FILES_STMT = (
		"UPDATE files"
		"	RIGHT JOIN inserts"
		"		ON inserts.path = files.path"
		"	SET"
		"		files.file_size = inserts.file_size,"
		"		files.modification_time = inserts.modification_time,"
		"		files.last_seen = {run},"
		"		files.to_be_read = 1,"
		"		files.to_be_compared = IF(files.modification_time = inserts.modification_time, 1, 0)"
		";"
	)


class TestCaseDontUpdateRandUpdateIndexInsertsPath(TestCaseDontUpdateRandUpdateNoJoinConditions):
	CREATE_INSERTS_TABLE_STMT = (
		"CREATE TABLE `inserts` ("
		"	`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,"
		"	`path` varbinary(4096) NOT NULL,"
		"	`modification_time` datetime(6) NOT NULL,"
		"	`file_size` bigint(20) unsigned NOT NULL,"
		"	PRIMARY KEY (`id`),"
		"	KEY `path` (`path`(2048))"
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
	)


class TestCaseDontUpdateRandUpdateNoInsertsConditions(TestCaseDontUpdateRand):
	UPDATE_EXISTING_FILES_STMT = (
		"UPDATE files"
		"	RIGHT JOIN inserts"
		"		ON inserts.path = files.path"
		"	SET"
		"		files.file_size = inserts.file_size,"
		"		files.modification_time = inserts.modification_time,"
		"		files.last_seen = {run},"
		"		files.to_be_read = 1,"
		"		files.to_be_compared = IF(files.modification_time = inserts.modification_time, 1, 0)"
		"	WHERE files.last_seen != {run}"
		";"
	)


class TestCaseDontUpdateRandUpdateNoInsertsConditionsAllInOn(TestCaseDontUpdateRandUpdateNoInsertsConditions):
	UPDATE_EXISTING_FILES_STMT = (
		"UPDATE files"
		"	RIGHT JOIN inserts"
		"		ON inserts.path = files.path AND files.last_seen != {run}"
		"	SET"
		"		files.file_size = inserts.file_size,"
		"		files.modification_time = inserts.modification_time,"
		"		files.last_seen = {run},"
		"		files.to_be_read = 1,"
		"		files.to_be_compared = IF(files.modification_time = inserts.modification_time, 1, 0)"
		";"
	)


class TestCaseDontUpdateRandUpdateNoInsertsConditionsAllInOnCompositeIndex(TestCaseDontUpdateRandUpdateNoInsertsConditionsAllInOn):
	CREATE_FILES_TABLE_STMT = (
		"CREATE TABLE `files` ("
		"	`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,"
		"	`rand` double NOT NULL,"
		"	`path` varbinary(4096) NOT NULL,"
		"	`modification_time` datetime(6) NOT NULL,"
		"	`file_size` bigint(20) unsigned NOT NULL,"
		"	`last_seen` bigint(20) unsigned NOT NULL,"
		"	`to_be_read` tinyint(3) unsigned NOT NULL DEFAULT 1,"
		"   `to_be_compared` tinyint(3) unsigned NOT NULL DEFAULT 0,"
		"	`checksum` varbinary(256) DEFAULT NULL,"
		"	`last_read` bigint(20) unsigned DEFAULT NULL,"
		"	PRIMARY KEY (`id`),"
		"	KEY `rand` (`rand`),"
		"	KEY `path_last_seen` (`path`(2048),`last_seen`)"
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
	)


class TestCaseDontUpdateRandRandInInserts(TestCaseDontUpdateRand):
	CREATE_INSERTS_TABLE_STMT = (
		"CREATE TABLE `inserts` ("
		"	`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,"
		"	`rand` double NOT NULL,"
		"	`path` varbinary(4096) NOT NULL,"
		"	`modification_time` datetime(6) NOT NULL,"
		"	`file_size` bigint(20) unsigned NOT NULL,"
		"	PRIMARY KEY (`id`)"
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
	)

	POPULATE_INSERTS_STMT = (
		"INSERT INTO `inserts`"
		"	(`rand`, `path`, `modification_time`, `file_size`)"
		"		SELECT"
		"			RAND(), `path`, `modification_time`, `file_size`"
		"		FROM `{src}`"
		";"
	)

	INSERT_NEW_FILES_STMT = (
		"INSERT INTO files (rand, path, file_size, modification_time, last_seen)"
		"	SELECT {src}.rand, {src}.path, {src}.file_size, {src}.modification_time, {run}"
		"	FROM {src}"
		"	LEFT JOIN files ON {src}.path = files.path"
		"	WHERE files.id IS NULL"
		";"
	)


class BenchResult(object):
	"""Container for metrics resulting from a single bench run.

	Attributes:
		scenario (Scenario): Scenario which produced the metrics.
		case (Case): Case which produced the metrics.
		update_existing_duration (float): Duration of the update_existing
			procedure.
		update_existing_analysis (str): Output of the ANALYZE FORMAT=json
			statement applied on the update_existing procedure.
		insert_new_duration (float): Duration of the insert_new procedure.
		insert_new_analysis (str): Output of the ANALYZE FORMAT=json statement
			applied on the insert_new procedure.
		delete_old_duration (float): Duration of the delete_old procedure.
		delete_old_analysis (str): Output of the ANALYZE FORMAT=json statement
			applied on the delete_old procedure.
	"""

	def __init__(self, scenario=None, case=None):
		self.scenario = scenario
		self.case = case

	def scenario_name(self):
		if self.scenario is None:
			return None

		try:
			return self.scenario.name
		except AttributeError:
			return self.scenario.__class__.__name__

	def case_name(self):
		if self.case is None:
			return None

		try:
			return self.case.name
		except AttributeError:
			return self.case.__class__.__name__

	def __str__(self):
		return repr(self)

	def __repr__(self):
		return "BenchResult({})".format({
			"scenario": self.scenario_name(),
			"case": self.case_name(),
			"update_existing_duration": self.update_existing_duration,
			"insert_new_duration": self.insert_new_duration,
			"delete_old_duration": self.delete_old_duration,
		})


class Scenario(object):
	DROP_TABLE_STMT = (
		"DROP TABLE `{name}`;"
	)

	OPTIMIZE_TABLE_STMT = (
		"OPTIMIZE TABLE `{name}`;"
	)

	RENAME_TABLE_STMT = (
		"RENAME TABLE `{src}` TO `{dst}`;"
	)

	def __init__(self, cnx, case, file_data_table="file_data"):
		self.cnx = cnx
		self.case = case
		self.file_data_table = file_data_table

	def setup(self):
		pass

	def setup_bench(self):
		pass

	def bench(self):
		result = BenchResult(
			scenario=self,
			case=self.case,
		)

		run_id = self.bench_run_id

		start = datetime.datetime.now()
		analysis = self.case.update_existing_files(self.cnx, run_id)
		end = datetime.datetime.now()

		result.update_existing_duration = (end - start).total_seconds()
		result.update_existing_analysis = analysis

		start = datetime.datetime.now()
		analysis = self.case.delete_old_files(self.cnx, run_id)
		end = datetime.datetime.now()

		result.delete_old_duration = (end - start).total_seconds()
		result.delete_old_analysis = analysis

		start = datetime.datetime.now()
		analysis = self.case.insert_new_files(self.cnx, run_id)
		end = datetime.datetime.now()

		result.insert_new_duration = (end - start).total_seconds()
		result.insert_new_analysis = analysis

		return result

	def cleanup_bench(self):
		pass

	def cleanup(self):
		pass

	def _drop_table(self, name):
		stmt = self.DROP_TABLE_STMT.format(name=name)

		self._execute_stmt(stmt)

	def _optimize_table(self, name):
		stmt = self.OPTIMIZE_TABLE_STMT.format(name=name)

		self._execute_stmt(stmt)

	def _rename_table(self, src, dst):
		stmt = self.RENAME_TABLE_STMT.format(src=src, dst=dst)

		self._execute_stmt(stmt)

	def _execute_stmt(self, stmt, isolation_level=None):
		self.cnx.start_transaction(isolation_level=isolation_level)
		cursor = self.cnx.cursor()

		cursor.execute(stmt)
		result = list(cursor)

		self.cnx.commit()
		cursor.close()

		return result


class EmptyScenario(Scenario):
	name = "empty"

	def setup(self):
		self.case.create_inserts_table(self.cnx)
		self.case.fast_populate_inserts(self.cnx, self.file_data_table)
		self.bench_run_id = 1

	def setup_bench(self):
		self.case.create_files_table(self.cnx)

	def cleanup_bench(self):
		self._drop_table("files")

	def cleanup(self):
		self._drop_table("inserts")


class NoInsertsScenario(Scenario):
	name = "no_inserts"

	def setup(self):
		self.case.create_inserts_table(self.cnx)
		self.case.fast_populate_inserts(self.cnx, self.file_data_table)

		self._rename_table("inserts", "inserts_run_1")

		self.case.create_inserts_table(self.cnx)
		self.case.fast_populate_inserts(self.cnx, self.file_data_table)
		self.bench_run_id = 2

	def setup_bench(self):
		self.case.create_files_table(self.cnx)

		self.case.fast_copy_to_files(self.cnx, "inserts_run_1", 1)

		self._optimize_table("files")

	def cleanup_bench(self):
		self._drop_table("files")

	def cleanup(self):
		self._drop_table("inserts")

		self._drop_table("inserts_run_1")


class BenchException(Exception):
	def __init__(self, msg, orig=None, scenario=None, case=None):
		full_msg = msg

		msg_bits = []

		if scenario is not None:
			try:
				scenario_name = scenario.name
			except AttributeError:
				scenario_name = scenario.__class__.__name__

			msg_bits.append("Scenario: {}".format(scenario_name))

		if case is not None:
			try:
				case_name = case.name
			except AttributeError:
				case_name = case.__class__.__name__

			msg_bits.append("Case: {}".format(case_name))

		if orig is not None:
			msg_bits.append("Original Exception: {}".format(orig))

		if msg_bits:
			full_msg += "\n" + (", ".join(msg_bits))

		super(BenchException, self).__init__(full_msg)

		self.msg = msg
		self.orig = orig
		self.full_msg = full_msg
		self.scenario = scenario
		self.case = case


class Bencher(object):
	def __init__(self, cnx, file_data_table="file_data"):
		self.cnx = cnx
		self.file_data_table = file_data_table

	def cross(self, scenario_classes, cases, n):
		results = []

		for scenario_class in scenario_classes:
			for case in cases:
				scenario = scenario_class(
					self.cnx,
					case,
					file_data_table=self.file_data_table,
				)

				try:
					bench_results = self._bench_scenario(scenario, n)

					results.extend(bench_results)
				except Exception as e:
					raise BenchException(
						"Exception occurred during scenario - case evaluation",
						orig=e,
						scenario=scenario,
						case=case,
					)

		return results

	def _bench_scenario(self, scenario, n):
		results = []

		scenario.setup()

		for _ in range(n):
			scenario.setup_bench()

			result = scenario.bench()
			results.append(result)

			scenario.cleanup_bench()

		scenario.cleanup()

		return results


def print_csv(results, f=None):
	f = f or sys.stdout

	w = csv.writer(f)

	w.writerow([
		"scenario",
		"case",
		"operation",
		"duration",
		"analysis",
	])

	for result in results:
		w.writerow([
			result.scenario_name(),
			result.case_name(),
			"update_existing",
			result.update_existing_duration,
			result.update_existing_analysis,
		])

		w.writerow([
			result.scenario_name(),
			result.case_name(),
			"delete_old",
			result.delete_old_duration,
			result.delete_old_analysis,
		])

		w.writerow([
			result.scenario_name(),
			result.case_name(),
			"insert_new",
			result.insert_new_duration,
			result.insert_new_analysis,
		])


def compile_scenario_class_registry(classes):
	def get_name(klass):
		try:
			return klass.name
		except AttributeError:
			return klass.__name__

	return dict((get_name(klass), klass) for klass in classes)


scenario_classes = compile_scenario_class_registry(
	[
		EmptyScenario,
		NoInsertsScenario,
	]
)


def compile_cases_registry(classes):
	def get_name(obj):
		try:
			return obj.name
		except AttributeError:
			return obj.__class__.__name__

	return dict((get_name(obj), obj) for obj in (klass() for klass in classes))


cases = compile_cases_registry(
	[
		TestCaseNoRand,
		TestCaseInitial,
		TestCaseInsertAllConditionsInOn,
		TestCaseInsertExists,
		TestCaseUpdateAllConditionsInOn,
		TestCaseUpdateNoJoinConditions,
		TestCaseUpdateIndexInsertsPath,
		TestCaseUpdateNoInsertsConditions,
		TestCaseUpdateNoInsertsConditionsAllInOn,
		TestCaseUpdateNoInsertsConditionsAllInOnCompositeIndex,
		TestCaseRandInInserts,
		TestCaseDontUpdateRand,
		TestCaseDontUpdateRandUpdateAllConditionsInOn,
		TestCaseDontUpdateRandUpdateNoJoinConditions,
		TestCaseDontUpdateRandUpdateIndexInsertsPath,
		TestCaseDontUpdateRandUpdateNoInsertsConditions,
		TestCaseDontUpdateRandUpdateNoInsertsConditionsAllInOn,
		TestCaseDontUpdateRandUpdateNoInsertsConditionsAllInOnCompositeIndex,
		TestCaseDontUpdateRandRandInInserts,
	]
)

def initialise_bencher(
	file_data_table='file_data',
	user=None,
	password=None,
	host='127.0.0.1',
	port=3306,
	database='test_schema',
):
	cnx = mysql.connector.connect(
		user=user,
		password=password,
		host=host,
		port=port,
		database=database
	)
	bencher = Bencher(cnx, file_data_table=file_data_table)

	return bencher

def list_cases():
	for key in cases.keys():
		print(key)

def list_scenarios():
	for key in scenario_classes.keys():
		print(key)

def bench_one(
	scenario,
	case,
	n=10,
	file_data_table='file_data',
	# MySQL connection options
	user=None,
	password=None,
	host='127.0.0.1',
	port=3306,
	database='test_schema',
):
	bencher = initialise_bencher(
		file_data_table=file_data_table,
		user=user,
		password=password,
		host=host,
		port=port,
		database=database,
	)

	scenario_class = scenario_classes[scenario]
	case = cases[case]

	results = bencher.cross(list(scenario_class), list(case), n)

	print_csv(results)


def bench_all(
	n=10,
	file_data_table='file_data',
	# MySQL connection options
	user=None,
	password=None,
	host='127.0.0.1',
	port=3306,
	database='test_schema',
):
	bencher = initialise_bencher(
		file_data_table=file_data_table,
		user=user,
		password=password,
		host=host,
		port=port,
		database=database,
	)

	results = bencher.cross(scenario_classes.values(), cases.values(), n)

	print_csv(results)


def bench_specific(
	n=10,
	file_data_table='file_data',
	# MySQL connection options
	user=None,
	password=None,
	host='127.0.0.1',
	port=3306,
	database='test_schema',
):
	bencher = initialise_bencher(
		file_data_table=file_data_table,
		user=user,
		password=password,
		host=host,
		port=port,
		database=database,
	)

	results = bencher.cross(scenario_classes.values(), cases.values(), n)

	print_csv(results)


parser = ArghParser()
parser.add_commands([list_cases, list_scenarios, bench_one, bench_all, bench_specific])


def main():
	parser.dispatch()


if __name__ == '__main__':
	main()
