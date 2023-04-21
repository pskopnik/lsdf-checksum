# System Design

## Goals

 * The primary goal of performing checksumming is to set-up and maintain a database of file content checksums.
   The purpose of this database is to be able to compare current file content with the database and detect file content corruptions.
 * Each execution of the checksumming procedure has one of the following intended targets:
   * **Full**: Compute checksums for all files.
     While writing back the checksums, corrupt files will be detected.
   * **Incremental**: Compute checksums only for files which have changed.
     This allows inexpensively keeping the checksum database in sync with the filesystem.
 * The checksumming procedure is performed on a live filesystem and should have negligible impact on other tasks running on the filesystem.
 * The checksumming procedure is intended to be run regularly to detect any corrupted files and be able to restore the content before the backups expire.

## Approach

 * One execution of the checksumming procedure is called a **run**.
 * A single coordinator/master controls the run, executing and delegating tasks.
   The master maintains the database and interacts with the Spectrum Scale management APIs.
 * The master distributes some work to a variable number of workers which may run on other hosts, most importantly the reading of files' content and computing of checksums thereof.
   Communication between master and worker is coordinated via a central queue system and happens via network.
   This allows the distribution of computationally expensive tasks across several hosts and thus enables scalability.
 * To keep the filesystem in a consistent state, a snapshot is created and used throughout the whole run.
   The snapshot is used both during meta data database synchronisation as well as checksum computing.
 * Meta data about files is kept in a persistent SQL database which the master connects to.
   For each file identified by its path on the filesystem, only relevant meta data is stored.
   This includes some attributes of the file in the filesystem along with the last known checksum further meta data related to the checksumming procedure.
 * Changed files are detected by comparing the `MODIFICATION_TIME` attribute.
 * SHA-1 is used to compute checksums of file content.

Each run consists of two phases:

 1. **Meta data database synchronisation**.
    File meta data is read from the filesystem and synchronised with the database.
    Changed files are detected and marked.
    After completion, the database represents the latest-known snapshot state of the filesystem.
    This phase is performed only on the master.
 2. **Work queue / checksum computation**.
    Checksums for files are computed.
    Computing of checksums takes place on an arbitrary number of worker hosts.
    The master node maintains a work queue containing *work packs*.

The entire process is visualised in the following pseudo-sequence diagram.
Its details will become clear from reading this document.

![Pseudo-Sequence Diagram of the Checksumming Procedure.](./sequence.svg)
_Pseudo-Sequence Diagram of the Checksumming Procedure._

There is also a [schema of the architecture](./architecture.svg) available.
The shapes labelled "Spectrum Scale ..." are interfaces provided by the IBM Spectrum Scale clustered filesystem.

### Meta data synchronisation

 * An SQL database is used to store all meta data.
   The store containing the meta data is referred to as the **meta data database**.
 * Meta data is primarily information about files, but the database also contains data about the checksumming runs as well as any warnings, i.e. corrupted files which have been detected.
 * File meta data is retrieved using the Spectrum Scale Policy Engine.
   * A `LIST` policy is used to create a list of files on the filesystem.
      Policies allow including arbitrary attributes about files.
      The Policy Engine allows performing list operations in a distributed fashion on any number of hosts.
   * The Policy Engine supports Quality-of-Service (QoS) classes, which limit the number of IOPS used by a command.
     The default QoS class used for applying list policies is `maintenance`, which is only allowed to utilise an admin-defined number of IOPS.

The meta data synchronisation has three steps:

 1. Applying the list policy: A `LIST` policy is applied using the `mmapplypolicy` command with the `defer` action on a cluster node.
    File attributes of interest are included via a `SHOW` statement.
    `mmapplypolicy` creates a file list containing one line for each file discovered.
 2. Reading the list and writing all files into the inserts table:
    The file list produced by `mmapplypolicy` is parsed and for each file found a row is inserted into the inserts table.
    Transactions or batchputs are used to reduce processing overhead for the database.
    Batchputs is the technique of combining many inserts into one `INSERT` statement by supplying multiple `VALUES` tuples.
 3. Synchronise the database by "merging" the inserts table into the Files table:
    The Files table is synchronised with the data in the inserts table by using SQL statement.
    First, all existing files are updated with the data from the inserts table.
    Then, files which only exist in the Inserts table are inserted into the Files table.
    Finally, files which have not been seen in the current run are deleted from the Files table.
    Afterwards the inserts table is cleaned (all inserted rows are deleted).

During inserting into the inserts table, the length of the file path is checked not exceed the length of the database `path` field.
A file with a longer path is dropped and a log warning is written.

#### SQL Schema

The proper UTF-8 encoding (`utf8mb4`) is used for all tables.

##### Files (table `files`)

The Files table is the central data store for both file meta data and checksums.
Rows in this table (the files) are maintained throughout runs, all information is updated, thus the Files table always represents a precise view of the filesystem (as seen by the last run).

Files are identified by their `path`.

Note that meta data synchronisation does not update the Files table directly but instead writes into the Inserts table and then "merges" rows over to this table.

 * `id` (`bigint(20) unsigned`) - Auto increment id, primary key.
 * `rand` (`double`) - Random double, uniformly chosen from `[0, 1)`.
   Some queries use this for ordering to ensure a uniform distribution of files (with regards to return order).
   Thus, a secondary index should be created on this column.
 * `path` (`varbinary(4096)`) - The path of the file.
   The path must be relative to the root of the checksumming process but begins with a slash.
   This column is used as a `JOIN ON` column, thus a secondary index should be created on this column.
 * `modification_time` (`datetime(6)`) - The modification time of the file as seen by the filesystem.
   This column is used to detect file changes.
 * `file_size` (`bigint(20) unsigned`) - The file size of the file, as seen by the filesystem.
 * `last_seen` (`bigint(20) unsigned`) - The id of the run this file was last seen in.
   This field is updated for all files during every runs.
 * `to_be_read` (`tinyint(3) unsigned`) - This column is a bit (`0` or `1`) indicating whether the file should be re-read and its checksum computed.
    It is only ever `1` while a run is ongoing.
    The field is set to `0` when the checksum is written.
 * `to_be_compared` (`tinyint(3) unsigned`) - This column is a bit (`0` or `1`) indicating whether the computed checksum should be compared to `checksum` before updating the field.
   If set to `1` a checksum warning is issued when the checksums don't match.
   It is only ever `1` while a run is ongoing.
   The field is set to `0` when the checksum is written.
 * `checksum` (`varbinary(64)`) - The checksum of the file content.
   Whenever this column is updated, `last_read` is updated as well, i.e. `last_read` is the id of the run this checksum was last computed in.
 * `last_read` (`bigint(20) unsigned`) - The id of the run this file was last read in.
   This field is always updated when the checksum for the file has been computed.

##### Inserts (table `inserts`)

The Inserts table is used as a temporary table.
All files which are retrieved during the meta data synchronisation phase from the filesystem are written into this table.
The rows are then "merged" into the Files table and later on deleted from the Inserts table.

 * `id` (`bigint(20) unsigned`) - Auto increment id, primary key.
    This column is not related to the `id` column of the Files table.
 * `path` (`varbinary(4096)`) - The path of file.
   This column is copied over to the Files table.
 * `modification_time` (`datetime(6)`) - The modification time of the file.
   This column is copied over to the Files table.
 * `file_size` (`bigint(20) unsigned`) - The file size of the file.
   This column is copied over to the Files table.

##### Runs (table `runs`)

The Runs table contains a row for each run.
The `id` column is referred to by the `last_seen`, `last_read`, ... columns of other tables.

 * `id` (`bigint(20) unsigned`) - Auto increment id, primary key.
 * `snapshot_name` (`varchar(256)`) - The name of the snapshot used for all operations throughout the run.
 * `snapshot_id` (`bigint(20) unsigned`) - The id of the snapshot used for all operations throughout the run.
 * `run_at` (`datetime(6)`) - The timestamp of the run.
   This is the "Created" attribute of the run's snapshot.
 * `sync_mode` (`varchar(20)`) - The run's sync mode.
   This is either `full` or `incremental`.
 * `state` (`varchar(20)`) - The state of the run.
   The state expresses the state of the data in the database.
   This information is sufficient to resume aborted (or failed) runs.

##### Lock (table `dblock`)

The Lock table allows locking of the database while a run is performed by the master.
An exclusive `WRITE` lock is acquired on the table whenever a run is about to start.
After the process has acquired the lock, it may write to the Runs table (insert new rows or change existing) as well as write data to the Inserts, Files and Checksum Warnings tables.

The Lock table contains no content.

 * `id` (`bigint(20) unsigned`) - Auto increment id, primary key.

##### Checksum Warnings (table `checksum_warnings`)

The Checksum Warnings table is used to write warnings about corrupted files.
For each corrupted file which is detected, a new row is inserted.
This table is meant for external consumption by monitoring / notification systems.
Warnings which are no longer required to be stored may be deleted by external tools.

 * `id` (`bigint(20) unsigned`) - Auto increment id, primary key.
 * `file_id` (`bigint(20) unsigned`) - The id of the file in the Files table.
 * `path` (`varbinary(4096)`) - The path of the file.
   Corresponds to the column of the same name in the Files table at the time of generation of this warning.
 * `modification_time` (`datetime(6)`) - The modification time of the file.
   Corresponds to the column of the same name in the Files table at the time of generation of this warning.
 * `file_size` (`bigint(20) unsigned`) - The file size of the file.
   Corresponds to the column of the same name in the Files table at the time of generation of this warning.
 * `expected_checksum` (`varbinary(64)`) - The expected checksum.
   This is the checksum written to the `checksum` column of the Files table in the last run reading this file.
 * `actual_checksum` (`varbinary(64)`) - The actual checksum computed in the run the corruption was discovered in.
 * `discovered` (`bigint(20) unsigned`) - The id of the run the corruption was discovered in.
   This is the run during which `actual_checksum` was computed.
 * `last_read` (`bigint(20) unsigned`) - The id of the run, the file was last read in before the corruption was discovered.
   This is the run during which `expected_checksum` was computed.
 * `created` (`datetime(6)`) - A timestamp of when the warning was created.
   This happens during the write back of checksums into the Files table.

### Work queue / checksum computation

During the work queue phase, the workers read files and compute the checksums of their content.
Due to the I/O throughput required for reading file data and the CPU required to compute checksums, this phase can be very resource-intensive.

Considerations:

 * Scalability and performance:
   The checksum computation should be highly scalable in terms of throughput (and thus total execution time).
   * Choosing an appropriately sized set of workers achieves the required throughput.
   * Workers can also be added and removed dynamically during ongoing checksum computation to react to changing conditions.
   * Operations on the master have are designed to be scalable as well, i.e. handle both very low and very high throughput.
   * Communication between master and workers is also designed to be scalable to a large number of workers and high throughput.
 * Resource consumption limits:
   Limited hardware resources are managed carefully so that consumption is reasonable.
   * Memory limitation:
     All components keep only a limited amount of data in memory.
     In particular, the required memory should be independent of the total number of files.
   * Graceful degradation:
     If a component becomes the bottleneck, the overall flow should slow down without incurring problems.
     In memory buffers should be kept at a reasonable size.
   * Impact on the filesystem:
     In most cases, the filesystem from which data is read is also in active use by other programs and users.
     The load of the checksum computation is configurable through adapting the set of workers as well as setting global limits in the master.
     Thus, checksum computation can be configured to have a negligible impact on any other load on the filesystem.
 * Resilience:
   Individual failures are recovered automatically as far as possible.
   If there are unexpected failures, the entire checksum system shuts down orderly (master process exits with error), so that operators can investigate and continue execution after mitigation.

#### Task Queue System

Primarily, the distributed aspects of the work queue phase are coordinated through a central task queue system.
Beside the actual queueing structure, there are further support structures for coordinating.

A [Redis](https://redis.io/) server powers the task queue system.
Master and worker connect to the Redis server to communicate (indirectly).
While Redis is not a message-queueing or task queue by itself, it offers a variety of operations on several data structures allowing to implement distributed algorithms.
The task queue implementation on top of Redis is [gocraft/work](https://github.com/gocraft/work).
It provides basic functionality around job processing by a set of workers.
Most importantly:

 * Jobs are enqueued with a name (= kind of the job) and parameters.
 * Each job name has its own queue and worker processes map the name to a specific processing function.
 * Errors and retries (with backoff/delay) are tracked for each job.
   * Each job execution is finished either successfully or with an error.
 * Once the maximum number of retries is exceeded, jobs are sent to a "dead job queue".
 * The global concurrency of jobs for a specific name can be limited.
 * Workers are registered as a `WorkerPool` which may consist of multiple concurrent executors (goroutines).
 * `WorkerPool`s check in regularly (heartbeat) and once a `WorkerPool` is detected as dead its ongoing jobs are re-enqueued.
   * A checksum worker can crash without much impact on the overall system.
 * Workers can pick up jobs from multiple queues, with configurable, probabilistic prioritisation between the queues.
 * Workers can be added and removed at any time.
 * The workers are tracked in Redis and can be queried, for example from the master.
 * Individual queues (i.e. job kinds) can be paused centrally, meaning workers will not dequeue jobs from there.

To circumvent limitations in what can be passed as job parameters, the actual parameters are marshalled according to the [MessagePack](https://msgpack.org/) format and encoded as a base64 string.

In addition to the queueing structures, a global set of potentially dynamic configuration options is stored in Redis.
The options are maintained by the master based according to policies and its own local configuration.
This global, dynamic configuration enables the master to coordinate workers beyond the model of the job-based task queue.
Workers have to regularly fetch relevant configuration options and make sure changes take effect.
At the moment, only a per-checksum-worker I/O throughput limit is stored.

Even during the work queue phase, the authoritative, consistent state of data is maintained in the SQL database.
Processing of a file has finished only once a checksum has been written back to the Files table.
Hence, a total loss (crash and loss of data) of the Redis server is no problem, and computation can continue after Redis is available again.
Thus, Redis may be configured without any [persistence (storage on disk)](https://redis.io/docs/management/persistence/).

##### Checksum Jobs and Job Production

Checksum Jobs instruct workers to read files and compute the checksum of file content.

A concern here is that the communication overhead caused by the queue becomes substantial for very small files.
Hence, each job is actually a _work pack_, containing an arbitrary list of files.
Each pack is built based on a minimum total file size threshold and a maximum file count.

On the master, the Producer component enqueues jobs to the Checksum Job queue.
The Producer iterates over the rows of to-be-read files in Files table in the order of the `rand` field.
It adds files to a work pack until either either threshold described above is reached.
Then it adds the job to the queue and beings filling the next work pack.
In combination, random order and multi-file work packs should ensure that very small files have a limited impact.

The queue in Redis must be explicitly represented, meaning each job is fully specified and enqueued, occupying space in memory of the Redis server.
The total list of files may be very large and the producer must enqueue with approximately the same rate as job consumption to prevent the queue from growing unboundedly.
In brief, an EWMA-based algorithm decides on the scheduling, inspired by the mechanism of estimating round-trip-times for congestion protocol in the Transmission Control Protocol.
The scheduling algorithm aims to keep the queue at an almost minimal size matching current job consumption while preventing the queue from becoming empty at any time.
During a fixed-duration warm-up phase, the scheduler checks very frequently the size of the queue to calculate consumption while keeping the queue well-filled.
Afterwards in the maintenance phase, the scheduling frequency is reduced (order of 10 s) and the queue length is kept at the exponentially-weighted-moving-average (EWMA) of recent consumption readings plus an additional safety buffer based on observed deviations in consumptions.
When the queue becomes empty, an exponential increase of the queue length performed, so that the scheduling adapts even to substantial scale-ups of workers.

Consumption on this queue may be paused to enforce backpressure when Write Back Jobs are not processed fast enough (see section below on Managing Performance).

The job name of Checksum Jobs is `ComputeChecksum` and it contains some common information about all files (filesystem name and snapshot name) and the list of files to be read and checksums of their content computed.
Each file in the list consists of the relative path inside the snapshot's directory and the id of the file in the meta data database.
The id is not required by workers but is included in the Write Back Job.

##### Write Back Jobs

Write Back Jobs contain a list of files and their checksum to be written into the meta data database by the master.

Once a worker has computed the checksum for all files in a Checksum Job, it will create a Write Back Job for these.
It does not have to create a single Write Back job.
If there are only few files where reading data has led to errors, the worker may split the job.
It will enqueue a new Checksum Job for the failed files, a Write Back job for the succeeded files and signals that the current Checksum Job has finished successfully.
To orderly complete the Checksum Job, all resulting jobs must have been successfully enqueued to their respective queues.

On the master, Write Back Jobs are dequeued and their content is written to an in-memory, bounded queue.
For simplicity, Write Back Jobs are thereafter immediately signalled to have been completed successfully, causing them to be removed from the task queue system in Redis.
Any retries have to be managed separately in the master.

The specific job name of the write back job contains the common information from the Checksum Job identifying the particular run of the checksum master.
This allows several master processes to submit Checksum Jobs on the single queue and receive results individually.

The job name of Write Back Jobs is `WriteBack-<FILESYSTEM_NAME>-<SNAPSHOT_NAME>` where `<FILESYSTEM_NAME>` and `<SNAPSHOT_NAME>` is replaced with information from the Checksum Job.
The job parameters consist of a list of files and their checksums.
Files are represented by their id, which is copied from the Checksum Job.

#### Monitoring Checksum Computation and Managing Performance

During the work queue phase, the queue's progress is monitored by the PerformanceMonitor on the master.
The component takes performance-related actions under some circumstances.

Firstly, if the writing of checksums back into the database becomes a bottleneck, the length of the write back queue, i.e. number of queued jobs will grow continuously.
To limit the amount of memory in use by the Redis server, the performance monitor will apply backpressure by pausing the Checksum Jobs queue and unpausing only once writing back has caught up.
The algorithm works according to a high-watermark/low-watermark mechanism on the number of queued jobs.

Secondly, a global I/O throughput limit can be configured centrally on the master.
This is implemented by the performance monitor by writing a per-worker-process limit to the Redis store.
The limit is updated whenever changes to the set of workers are detected.
The limit is not specific to a worker, so all workers get the same limit of `total_throughput / worker_count` regardless of processing strength.
Also, it is not enforced through a reservation system but simply published to the work queue configuration in Redis.
All workers regularly fetch the limit and apply it to their processing.

Future goals of the performance monitor include collecting performance metrics from the workers and publishing globally aggregated metrics.

#### Computing Checksums on Workers

Each checksum worker process starts a work queue worker pool with one or multiple workers (goroutines).
Each handles a single job at a time.

The only job kind is the Checksum Job where files are read, checksums computed and the results are submitted as a write back job.

Checksum jobs do not contain the absolute path to the files and these may differ from host to host.
To construct the full path to a file, local information about the mounted filesystem specified in the Checksum Job has to be retrieved.
This information is loaded into a cache with an expiry date so that these relative expensive commands are executed at most every 30 minutes or so.

I/O throughput limitations are also applied during reading.
At this time, all limitations can be evaluated locally, there is no need to communicate with other processes.
Each application `read` call to a file has to pass a rate limiting mechanism.
A [token bucket](https://en.wikipedia.org/wiki/Token_bucket) implementation is employed with a configurable burst and a refill rate derived from the configured throughput limit.
The burst is the maximum size of a `read` call on the OS file.
This call is delayed until the bucket has reached sufficient capacity.
If the application attempts a `read` call with a size larger than the burst, this will result in multiple `read` calls on the OS file, each with a maximum size of the burst.
The approach applies also to multiple buckets to adhere to multiple independent limits.
Here a `read` on the OS file is delayed until all buckets have refilled sufficiently.
Changing limit and burst during runtime is possible.

There are two I/O throughput limits applied.
Firstly, each worker has an optional limit specified via its configuration file.
That represents the total throughput limit for the process.
Secondly, the worker will retrieve the per-process limit from the global configuration in Redis.
This limit enforces a global throughput limit configured on the master.

Linux syscalls have an upper limit on the length of a file path passed to `open()` etc..
This limit is usually 4096 bytes.
As paths in the database have to be prefixed with the path to the local snapshot directory, the total file path may exceed this limit.
Workers will attempt to create and chain multiple symlinks to construct a path to the file which is shorter than the limit imposed by syscalls.
Further limits (such as number of symlinks resolved in a syscall) come into play here and this approach is not guaranteed to work under all circumstances.

### SQL Database

A single SQL database holds all persistent data required for the checksumming procedure.
MySQL/MariaDB with the InnoDB storage engine was chosen as the database server, although a switch to another server would be possible.
Only the master accesses the database.

A dedicated, separately running database server was chosen for the data backend because it solves problems around data consistency and persistence and gives much flexibility around how to query data.
The scalability, particularly in terms of data volume, of wide-spread database servers such as MySQL has also been proven.
In theory, the separate database server would also allow other programs to access the file meta data, although this would have to be carefully designed to not interfere with ongoing checksumming runs.

The SQL database also provides the final level of resilience to the checksum system.
Its strong consistency characteristics ensure that stored data represents a fully consistent snapshot of all data.
All updates to the database are designed to maintain the data schema's invariants.
Taken together, this means that data in the database is always correct and that user-instructed stops and even crashes of the checksum master will always allow continuing the run without complications.
Only time is lost, for example some files may be re-read during the checksum computation phase and the meta data synchronisation phase has to be fully repeated.

When querying data, a particular challenge is the size of the Files table meaning a huge amount of individuals rows may be touched and returned by individual queries.
This is a problem, as the amount of bookkeeping increases linearly with the number of rows involved in an operation.
MySQL is an Online Transaction Processing (OLTP) system, which takes great care to provide guarantees around transactions and is optimised for complex operations on a small number of rows.
For checksumming runs, instead relatively simple operations are executed on a large number of rows.
To mitigate problems arising from this contrast, all operations on the database are designed to be relatively short-lived transactions which only touch a moderate number of rows, independent of the total size of the Files table.
_Chunking_ splits the total dataset into fixed-size smaller sets and executes each query on such chunks individually.
It works best when splitting along an index, allowing MySQL to optimise execution for a fast lookup and then walking the index.
In SQL, limiting a query to the chunk is achieved by adding a filter conditions, for example `WHERE id >= ? AND id < ?`.
Chunk boundaries can be determined beforehand through a query only using the indexed column by `SELECT`ing the last `id` within a chunk size `LIMIT`ed region starting with the last returned `id`.
The chunk size is chosen to balance efficiency in terms of total execution time with chunk size.

Data mutations (insert, update, delete) are chunked in the same way.
Mutations require even more bookkeeping, updating rows which may potentially be accessed in other transactions and triggering updates to index structures.
This work has been observed to be CPU-bound on the database server in many cases.
By concurrently executing mutating operations on several connections, the overall operation throughput can be greatly improved.
A common concurrency hint is associated with the `meda` database instance and other components may rely on this.
The concurrency value should be related to the available CPU cores on the database server.

SQL queries and statements are benchmarked extensively to identify the most efficient variant and to understand trade-offs between its parameters, for example transaction size, concurrency and row mutations per statement.
Relevant parameters should be exposed to operators, so that they can tune the system to their environment.

### Run State

In the Runs table, each run has an associated state which describes the current phase within the checksumming procedure.
Because the field is updated during each transition, it allows continuing a run even if the master process crashes unexpectedly.

Another purpose of the state field is to ensure integrity of the file meta data.
The Files table always describes the most up-to-date known state of the filesystem.
Hence, only the latest run may modify it.
Upon starting a new run via the master, the state of all previous runs is checked.
If there are any unresolved runs, the user first has to decide whether to continue or abort those before the new run can start.

Aborting a run means it can no longer be continued and the next run may take over updating the database.
This state can only be reached through explicit user action.
Even a partially executed run may be aborted without causing data integrity problems, as each rows in the Files table is self-contained.

The run state follow a state machine of defined transitions.
Any execution of the checksum master targets a state where the execution will stop (if not interrupted).
Reaching one of the terminal states means no more work can be performed and the run is resolved.

```plain
initialised  --> snapshot --> medasync --> workqueue --> cleanup --> finished
   |              |              |              |           |
   |              |    +---------|--------------+-----------+
   |              |    |         |
   |              v    v         v
   |    aborting-snapshot <-- aborting-medasync
   |                 |
   +-----------------+----+
                          v
                       aborted
```

 * `initialised` - Initial state of each run.
   A snapshot name (with a random component) is generated and stored in the run.
 * `snapshot` - Creates a snapshot in the filesystem with the specified name and stores its information in the run.
 * `medasync` - Performs the meta data synchronisation.
 * `workqueue` - Performs the checksum computation and writes checksums back into the Files table.
   During this processing, checksum warnings are generated for each detected mismatch.
 * `cleanup` - Deletes the snapshot from the filesystem.
 * `finished` - _Terminal state._
 * `aborting-medasync` - Cleans up intermediary data (Inserts table) potentially created during the medasync phase.
 * `aborting-snapshot` - Deletes the snapshot from the filesystem.
 * `aborted` - _Terminal state._
