# System Design

## Goals

 * The primary goal is to set-up and maintain a database of file content
    checksums.
    The purpose of this database is to be able to compare current file content
    with the database and detect file content corruptions.
 * Each checksumming run has one of the following intended targets:
   * **Full**: Calculate checksums for all files. While writing back the
      checksums, corrupt files will be detected.
   * **Incremental**: Calculate checksums only for files which have changed.
      This allows inexpensively keeping the database in sync.
 * The checksumming process is performed on a live file system and should have
    negligible impact on other tasks running on the file system.
 * The checksumming process is intended to be run regularly to detect any
    corrupted files and be able to restore the content before the backups
    expire.

## Approach

One execution of the checksumming process is called a **run**.

 * To keep the file system in a consistent state, a snapshot is created and used
    throughout the whole run. The snapshot is used both during meta data
    database synchronisation as well as checksum calculations.
 * Changed files are detected by comparing the `MODIFICATION_TIME` attribute.
 * SHA-1 is used to calculate checksums of file content.

Each run consists of two phases:

 1. **Meta data database synchronisation**. File meta data is read from the
     file system and synchronised with the database. Changed files are detected
     and marked. This phase is performed only on the master.
 2. **Work queue / checksum calculation**. Checksums for files are calculated.
     The calculation of checksums takes place on an arbitrary number of worker
     nodes. The master node maintains a work queue containing *work packs*.

### Meta data synchronisation

 * A SQL database is used to store all meta data. The store containing the meta
    data is referred to as the **meta data database**.
 * Meta data is primarily file meta data, but the database also contains data
    about the checksumming runs as well as any warnings, i.e. corrupted files
    which have been detected.
 * File meta data is retrieved using the Spectrum Scale Policy Engine.
   * A `LIST` policy is used to create a list of files on the file system.
      Policies allow including arbitrary attributes about files.
      The Policy Engine allows performing list operations in a distributed
      fashion on any number of nodes.
   * The Policy Engine supports Quality-of-Service (QoS) classes, which limit
      the number of IOPS used by a command. The default QoS class used for
      applying list policies is `maintenance`, which is only allowed to utilise
      an admin-defined number of IOPS.

The meta data synchronisation has three steps:

 1. Applying the list policy: A `LIST` policy is applied using the
     `mmapplypolicy` command with the `defer` action on a cluster node. File
     attributes of interest are included via a `SHOW` statement. `mmapplypolicy`
     creates a file list containing one line for each file discovered.
 2. Reading the list and writing all files into the inserts table: The file list
     produced by `mmapplypolicy` is parsed and for each file found a row is
     inserted into the inserts table. Transactions or batchputs are used to
     reduce processing overhead for the database. Batchputs is the technique of
     combining many inserts into one `INSERT` statement by supplying multiple
     `VALUES` tuples.
 3. Synchronise the database by "merging" the inserts table into the Files
     table: The Files table is synchronised with the data in the inserts table
     by using SQL statement. First, all existing files are updated with the
     data from the inserts table. Then, files which only exist in the Inserts
     table are inserted into the Files table. Finally, files which have not been
     seen in the current run are deleted from the Files table. Afterwards the
     inserts table is cleaned (all inserted rows are deleted).

#### SQL Scheme

Ensure that a proper UTF-8 encoding (utf8mb4) is used.

##### Files (table `files`)

The Files table is the central data store for both file meta data and checksums.
Rows in this table (the files) are maintained throughout runs, all information
is updated, thus the Files table always represents a precise view of the file
system (as seen by the last run).

Files are identified by their `path`.

Note that meta data synchronisation does not update the Files table directly
but instead writes into the Inserts table and then "merges" rows over to this
table.


 * `id` (`bigint(20) unsigned`) - Auto increment id, primary key.
 * `path` (`varbinary(4096)`) - The path of the file. The path must be relative
    to the root of the checksumming process but begins with a slash. This column
    is used as a `JOIN ON` column, thus a secondary index should be created on
    this column.
 * `modification_time` (`datetime(6)`) - The modification time of the file as
    seen by the file system. This column is used to detect file changes.
 * `file_size` (`bigint(20) unsigned`) - The file size of the file, as seen by
    the file system.
 * `last_seen` (`bigint(20) unsigned`) - The id of the run this file was last
    seen in. This field is updated for all files during every runs.
 * `to_be_read` (`tinyint(3) unsigned`) - This column is a bit (`0` or `1`)
    indicating whether the file should be re-read and its checksum calculated.
    It is only ever `1` while a run is ongoing. The field is set to `0` when the
    checksum is written.
 * `checksum` (`varbinary(64)`) - The checksum of the file content. Whenever
    this column is updated, `last_read` is updated as well, i.e. `last_read` is
    the id of the run this checksum was last calculated in.
 * `last_read` (`bigint(20) unsigned`) - The id of the run this file was last
    read in. This field is always updated when the checksum for the file has
    been calculated.

##### Inserts (table `inserts`)

The Inserts table is used as a temporary table. All files which are retrieved
during the meta data synchronisation phase from the file system are written into
this table. The rows are then "merged" into the Files table and later on
deleted from the Inserts table.

 * `id` (`bigint(20) unsigned`) - Auto increment id, primary key. This column is
    not related to the `id` column of the Files table.
 * `path` (`varbinary(4096)`) - The path of file. This column is copied over to
    the Files table.
 * `modification_time` (`datetime(6)`) - The modification time of the file. This
    column is copied over to the Files table.
 * `file_size` (`bigint(20) unsigned`) - The file size of the file. This column
    is copied over to the Files table.
 * `last_seen` (`bigint(20) unsigned`) - The id of the run this file was
    retrieved in, i.e. the current (ongoing) run. This column is copied over to
    the Files table.

##### Runs (table `runs`)

The Runs table contains a row for each run. The `id` column is referred to by
the `last_seen`, `last_read`, ... columns of other tables.

 * `id` (`bigint(20) unsigned`) - Auto increment id, primary key.
 * `snapshot_name` (`varchar(256)`) - The name of the snapshot used for all
    operations throughout the run.
 * `snapshot_id` (`bigint(20) unsigned`) - The id of the snapshot used for all
    operations throughout the run.
 * `run_at` (`datetime(6)`) - The timestamp of the run. This is the "Created"
    attribute of the run's snapshot.
 * `sync_mode` (`varchar(20)`) - The run's sync mode. This is either `full` or
    `incremental`.
 * `state` (`varchar(20)`) - The state of the run. The state expresses the
    state of the data in the database. This information is sufficient to
    resume aborted (or failed) runs.

##### Checksum Warnings (table `checksum_warnings`)

The Checksum Warnings table is used to write warnings about corrupted files.
For each corrupted file which is detected, a new row is inserted. This table
is meant for external consumption by monitoring / notification systems.
Warnings which are no longer required to be stored may be deleted by external
tools.

 * `id` (`bigint(20) unsigned`) - Auto increment id, primary key.
 * `file_id` (`bigint(20) unsigned`) - The id of the file in the Files table.
 * `path` (`varbinary(4096)`) - The path of the file. Corresponds to the column
    of the same name in the Files table at the time of generation of this
    warning.
 * `modification_time` (`datetime(6)`) - The modification time of the file.
    Corresponds to the column of the same name in the Files table at the time of
    generation of this warning.
 * `file_size` (`bigint(20) unsigned`) - The file size of the file. Corresponds
    to the column of the same name in the Files table at the time of generation
    of this warning.
 * `expected_checksum` (`varbinary(64)`) - The expected checksum. This is the
    checksum written to the `checksum` column of the Files table in the last run
    reading this file.
 * `actual_checksum` (`varbinary(64)`) - The actual checksum calculated in the
    run the corruption was discovered in.
 * `discovered` (`bigint(20) unsigned`) - The id of the run the corruption was
    discovered in. This is the run during which `actual_checksum` was
    calculated.
 * `last_read` (`bigint(20) unsigned`) - The id of the run, the file was last
    read in before the corruption was discovered. This is the run during which
    `expected_checksum` was calculated.
 * `created` (`datetime(6)`) - A timestamp of when the warning was created. This
    happens during the write back of checksums into the Files table.

### Work queue / checksum calculation

#### Performance Considerations

The calculation of checksums should have negligible impact on the performance
of any actual tasks running on the file system.

Some considerations:

 * Limiting the network overhead and latency incurred by fetching jobs from the
   global work queue.
   * Instead of putting single files in the queue, **work packs** are used.
      A work pack is a list (of arbitrary length) of files for which checksums
      are to be calculated. Work packs are assembled and enqueued by the master.
   * The master creates work packs as to limit communication overhead between
      workers and the work queue. Each work pack is assembled to contain files
      with a total file size greater than a specific threshold. This avoids
      cases when the worker only receives very small files from the queue and
      the network overhead has significant impact on the throughput.
 * Limiting memory demand of the work queue.
   * The master process enqueues only a certain number of work packs, further
      work packs are only enqueued when the queue length drops below a
      threshold. The threshold is adjusted to the current processing speed.
      This way, the entire work queue can be kept in memory.
   * A (client side) cursor is used to iterate over the row of the Files table
      of the database.  When the master needs to enqueue further files, the next
      rows returned by the cursor are used to create work packs.
      During incremental runs, only rows with the `to_be_read` flag are
      considered.
 * Limiting I/O throughput.
   * The I/O throughput limit is enforced by a low-level wrapper around a file
      reader for each checksum worker. The reader plus wrapper are referred to
      as the **rate limited reader**. The rate limit component is reused by
      each checksum worker. This way, the throughput limit is upheld even when
      switching to a new file.
   * Workers fetch a global throughput limit per worker from the redis / ledis
      database. The throughput is expressed as the maximum number of bytes per
      second allowed to be read. The throughput limit is regularly re-fetched.
   * The throughput is split between all the worker's checksum workers. Each
      worker then enforces the throughput by using a rate limited reader, as
      described above.

#### Resilience

 * If any sort of error occurs during processing of a work pack which the
    worker cannot recover from, the work pack is re-queued. The number of (max)
    retries can be configured per queued item, i.e. work pack. It is enforced
    by the underlying queue library.
 * Workers are regularly checking in with the work queue (heartbeat). If no
    heartbeat is received for some time, all jobs currently being processed by
    the worker are re-queued. This allows for a worker crashing or a system
    being restarted without much impact on the checksum calculations.
