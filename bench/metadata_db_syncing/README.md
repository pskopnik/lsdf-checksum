# Metadata DB Syncing Benchmarking

This benchmark measures the time different SQL commands take during the
synchronisation stage.

Files and their associated meta data is read from a file list created by a list
policy and inserted into the `inserts` table.
The data then has to be synchronised with the `files` table. This can be
achieved with three SQL commands.

 1. Copy all **new** files from `inserts` to `files`. For all these files,
		 `to_be_read` is set to `1` (calculating an initial checksum is necessary).
 2. Update all **existing** files in the `files` table. If the
		 `modification_time` field has changed, `to_be_read` is set to `1`.
		 Otherwise it is set to `0`.
 3. Delete all untouched files from the `files` table. These files were not
		 included in the policy's file list and have thus been deleted from the
		 file system.

This benchmark measures the time it takes to perform the commands performing
each step. Different methods may be available for a step.

## Requirements

 * Python3.
 * PyPi package `mysql-connector-python` installed.
 * PyPi package `argh` installed.
 * MySQL server running on `localhost`. Authentication with `root` and no
		password possible.
 * Tables `inserts` and `files` available in database `test_schema`.
 * Data available in table `inserts` with `last_seen = 1`. All these rows will
		be used during benchmarking.

With python3 installed, the PyPi packages can be installed in a virtual env as
follows:
```
python3 -m venv venv
. venv/bin/activate
pip install --upgrade pip wheel
pip install mysql-connector-python argh
```

Setup database as follows:
```
CREATE DATABASE test_schema;
USE test_schema;

CREATE TABLE `inserts` (
	`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
	`rand` double NOT NULL,
	`path` varchar(4096) NOT NULL,
	`modification_time` datetime(6) NOT NULL,
	`file_size` bigint(20) unsigned NOT NULL,
	`last_seen` bigint(20) unsigned NOT NULL,
	PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `files` (
	`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
	`rand` double NOT NULL,
	`path` varchar(4096) NOT NULL,
	`modification_time` datetime(6) NOT NULL,
	`file_size` bigint(20) unsigned NOT NULL,
	`last_seen` bigint(20) unsigned NOT NULL,
	`to_be_read` tinyint(3) unsigned NOT NULL DEFAULT 1,
	`checksum` varbinary(64) DEFAULT NULL,
	`last_read` bigint(20) unsigned DEFAULT NULL,
	PRIMARY KEY (`id`),
	KEY `rand` (`rand`),
	KEY `path` (`path`(1024))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
```

The script `insert.py` may be used to insert files read from a policy file list
into the `inserts` table:
```
python insert.py ~/filelist.files
```

The following policy creates a file list in the expected format:
```
RULE 'listFilesRule'
LIST 'files'
SHOW('|' || varchar(file_size) || '|' || varchar(modification_time) || '|')
```

The policy can be applied as follows (the policy has to be stored in
`list_defer_policy.txt`, the output will be written to `~/filelist.list.files`):
```
mmapplypolicy /gpfs2/emptyfiletree -I defer -f ~/filelist -P list_defer_policy.txt
```

## Benchmarks

Benchmarks are performed by running `bench.py`.
The script will output timings for repeated experiments.
All timings are represented as the duration in seconds (mostly with a decimal
point).

All benchmarks wait at several points for 5 minutes. This is intended to
simulate the "quiet" period between multiple runs and allow the database engine
to perform any sort of maintenance tasks.
This is probably not necessary (and also 5 minutes are probably not long
enough). To speed up benchmark execution the `time.sleep(...)` instructions may
be removed.

### `empty`

This benchmark performs insert methods on an empty `files` table:

 * `insert_in`: Copies new files from the `inserts` table into the `files`.
    Uses the SQL `IN` expression to filter out existing files.
 * `insert_join`: Copies new files from the `inserts` table into the `files`.
    Uses a `JOIN` to filter out existing files.

Perform by running:
```
python bench.py bench-empty
```

## `no_inserts`

This benchmark performs update, insert and delete methods on a pre-filled
`files` table.
As a setup step, all files from `inserts` are copied to `files`. The
`last_seen` value is then increased to `2` (from `1`) for all `inserts`
files. The `files` table now contains all files from the original `inserts`
table. When running this benchmark, no new files are inserted into `files`,
only existing ones are updated. No files are deleted.

 * `update`: Updates all existing files using a `JOIN`.
 * `insert_in`: Copies new files from the `inserts` table into the `files`.
    Uses the SQL `IN` expression to filter out existing files.
 * `insert_join`: Copies new files from the `inserts` table into the `files`.
    Uses a `JOIN` to filter out existing files.
 * `delete`: Removes all files which don't have an up-to-date `last_seen`
    value.

Perform by running:
```
python bench.py bench-no-inserts
```
