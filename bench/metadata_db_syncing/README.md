# Metadata DB Syncing Benchmarking

This benchmark measures the time different SQL commands take during the
synchronisation stage.

Files and their associated meta data is read from a file list created by a list
policy and inserted into the `file_data` table.
Different benchmark scenarios use the `file_data` table to populate the
`inserts` table.
The data then has to be synchronised with the `files` table. This is achieved
with three SQL commands.

 1. Update all **existing** files in the `files` table. If the
     `modification_time` field has changed, `to_be_read` is set to `1`.
     Otherwise it is set to `0`.
 2. Copy all **new** files from `inserts` to `files`. For all these files,
     `to_be_read` is set to `1` (calculating an initial checksum is necessary).
 3. Delete all untouched files from the `files` table. These files were not
     included in the policy's file list and have thus been deleted from the
     file system.

This benchmark measures the time it takes to perform the commands performing
each step. Different methods may be available for a step.

## Requirements

 * Python3.
 * PyPi package `mysql-connector-python` installed.
 * PyPi package `argh` installed.
 * MySQL server running on `localhost`, authentication for example with user
    `root` and no password.
 * Table `file_data` available in database `test_schema`.
 * Data available in table `file_data`. All these rows will be used during
    benchmarking.

With python3 installed, the PyPi packages can be installed in a virtual env as
follows:
```
python3 -m venv venv
. venv/bin/activate
pip install --upgrade pip wheel
pip install mysql-connector-python argh
```

The script `import_data.py` may be used to create the `file_data` table and to
insert file data read from a policy file list into this table:
```
python import_data.py ~/filelist.list.files
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
mmapplypolicy /gpfs2/sub/file/tree -I defer -f ~/filelist -P list_defer_policy.txt
```

## Benchmarks

Benchmarks are performed by using `bench.py`. The script has multiple
subcommands.

Each experiment is performed using a combination of a scenario and a test case
and consists of the execution of different operations. The scenario simulates
the state of the data in the database and also determines which operations are
performed. Each test case comprises a specific implementation of each
operation.

For each experiment the duration of each operation is measured. Experiments are
repeated `n` times. The output of benchmarks is a csv file with all. Each
duration is represented as a decimal of its number of seconds.

The usage of the `bench.py` can be discovered using its `--help` flag:

```bash
python bench.py --help
```

### Scenarios

Each scenario simulates a state of the data currently in the database. Each
scenario also runs a specific set of operations. These result in methods of the
test case being called.

#### `empty`

The `empty` scenario simulates an empty `files` table. The information for each
file has to be inserted into this empty table.

The scenario performs the standard synchronise meta data operations and computes
the time each operation took:

 1. Updating of existing files (method `update_existing_files()`, key
    `update_existing` in results)
 2. Deleting of old files, i.e. files which have been deleted from the file
    system (method `delete_old_files()`, key `delete_old` in results)
 3. Inserting of new files, i.e. files which have been newly created in the
    file system (`insert_new_files`, key `insert_new` in results)

In the `empty` scenario only the inserting operation results in changes of the
data set.

#### `no_inserts`

The `no_inserts` scenario simulates a `files` table containing all files with
up-to-date information. There are no new files to be inserted and no files have
been deleted either.

The scenario performs the standard synchronise meta data operations and computes
the time each operation took:

 1. Updating of existing files (`update_existing_files`)
 2. Deleting of old files, i.e. files which have been deleted from the file
    system (`delete_old_files`)
 3. Inserting of new files, i.e. files which have been newly created in the
    file system (`insert_new_files`)

In the `no_inserts` scenario only the updating operation results in changes of
the data set and no file data is updated except the `last_seen` field, which is
incremented for all files.

## Helpers

### Subset of file_data table

The following queries copy a random subset of count 1000 of the `file_data`
table into a new table named `file_data_small`:

```sql
# Create new table `file_data_small`:

# Amend the AUTO_INCREMENT number, e.g. via SHOW CREATE TABLE `file_data`;
CREATE TABLE `file_data_small` (
    `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
    `path` varbinary(4096) NOT NULL,
    `modification_time` datetime(6) NOT NULL,
    `file_size` bigint(20) unsigned NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=<AUTO_INCREMENT_NUMBER_FROM_FILE_DATA> DEFAULT CHARSET=utf8mb4;

# Or (does not set the AUTO_INCREMENT number):

CREATE TABLE `file_data_small` LIKE `file_data`;

INSERT INTO file_data_medium (id, path, modification_time, file_size)
    SELECT
        file_data.id, file_data.path, file_data.modification_time, file_data.file_size
    FROM file_data
    RIGHT JOIN (
        SELECT DISTINCT id FROM (
            SELECT
                FLOOR(
                    1 + RAND() * (
                        SELECT MAX(id) FROM file_data
                    )
                ) as id
                FROM file_data LIMIT 2000
        ) AS random_ids_source LIMIT 1000
    ) AS random_ids ON file_data.id = random_ids.id
;
```
