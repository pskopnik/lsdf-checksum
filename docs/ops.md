# Ops Documentation

This document gives on overview of the lsdf-checksum system from an operations
standpoint.
The operations interface (CLI and config files) is documented as well as details
about the depended upon services (MySQL, Redis and IBM Spectrum Scale).

An operational lsdf-checksum system consists of one master and an arbitrary
number of workers. The master maintains a database table of files which are
monitored. The master initiates and manages "runs". Each run consists of two
phases. First, the master scans the file system and updates the table of
files. Then, the checksums are computed for all or a subset of files. Checksum
computation is performed by a pool of workers. Master and workers communicate
via the "work queue" powered by Redis. While updating the table of files with
the calculated checksums, checksum warnings are emitted, if a discrepancy is
discovered.

The master is the central component of the lsdf-checksum system. The process
representing the master is started manually (or via cron / systemd / ...) and
stops as soon as all files have been processed and the run is finished. The
workers only have to be running while the master is running. Alternatively,
the worker processes may run permanently as daemons, as the worker process is
rather lightweight. The worker pool and work queue scheduling is robust to
changes, so workers may be added or removed (or crash) at any time.

Each pool of workers is associated with only one master. And each master
configuration supports only one sub-tree of a single file system. This is a
current limitation of the system. However:

 * The same machines may be used in several worker pools. One worker process
   per master may be started.
 * The same MySQL and Redis servers may be used by multiple master
   configurations. Isolation is achieved through separate databases or table /
   key prefixing.

## IBM Spectrum Scale (GPFS)

Both the master and workers require mount access to the IBM Spectrum Scale file
system to be monitored. The master also performs management commands on the file
system: The policy engine is used (via `mmapplypolicy`) to retrieve a file list
of the files in the file system. Additionally, a snapshot is created for each
run and deleted thereafter (via `mm*snapshot`).

The lsdf-checksum system supports any type of file system configuration where
global snapshots can be created. This includes file systems with more than one
independent file sets.

lsdf-checksum supports monitoring of a sub path of a file system. The options
`master.filesystemname` and `master.filesystemsubpath` must be set to perform
file system scanning.

### Likely Problems

 * The Spectrum Scale policy engine can be configured to run in a distributed
   fashion on all or some nodes. See the documentation of the
   [mmapplypolicy][spectrum-scale-mmapplypolicy] command for more information.
   The options `master.medasync.nodelist` and
   `master.medasync.globalworkdirectory` determine the behaviour.

 * The file list for large file systems will be rather large. Storing the file
   list in the OS `/tmp` directory may lead to errors, as `/tmp` is not large
   enough. Through the `master.medasync.temporarydirectory` option, a directory
   on another file system with sufficient space may be used (e.g. a directory in
   a Spectrum Scale file system). Even storing the file list in the same file
   system as being scanned is possible, as the policy is applied on a snapshot.
   The directory specified is used as the `LocalWorkDirectory` (flag `-s`) of
   the [mmapplypolicy][spectrum-scale-mmapplypolicy] command and for storing the
   generated file list until all files have been inserted into the database
   (`inserts` table).

 * Not every command shipping with Spectrum Scale has a well-defined interface.
   Hence, the lsdf-checksum system sometimes relies on parsing regular command
   output. Any change in the formatting may break the parsing and result in
   errors.

 * The lsdf-checksum outputs the error code if the execution of a Spectrum Scale
   command fails. The Spectrum Scale knowledge centre has a list of all
   [Messages][spectrum-scale-messages] as well as documentation on the structure
   of error codes.

[spectrum-scale-mmapplypolicy]: https://www.ibm.com/support/knowledgecenter/STXKQY_4.2.3/com.ibm.spectrum.scale.v4r23.doc/bl1adm_mmapplypolicy.htm
[spectrum-scale-messages]: https://www.ibm.com/support/knowledgecenter/STXKQY_4.2.3/com.ibm.spectrum.scale.v4r23.doc/bl1pdg_message.htm

## MySQL

The lsdf-checksum master requires a MySQL server to store, retrieve and maintain
all persistent data. This includes meta data about runs, checksum warnings as
well as file data (including file content checksums).

The MySQL deployment in use should be planned carefully:

 * The master stores all file data in the database. Furthermore, all existing
   files are temporarily inserted into an `inserts` table during each run.
   **Thus, the amount of storage required can be significant.**
 * The master performs operations of batch-type. This includes performing
   join-updates of the `files` table as well as random row updates. Some
   operations performed by the master can be performed concurrently.
   **The MySQL server will utilise significant CPU-% and IOPS.**

For these reasons, a dedicated system is recommended for hosting the database
server.

Multiple master configuration may use the same MySQL server. The master
configurations may be isolated by creating a database for each master
configuration and specifying the database as part of the `db.datasourcename`
option. Alternatively, each master configuration may use a different table
prefix by specifying the `db.tableprefix` option.

The tables used by the master are managed by the master. Migrations are
performed automatically when a master process connects to the database.

This documents refers to the [MariaDB documentation][mariadb-docs] for specific
documentation settings.

[mariadb-docs]: https://mariadb.com/kb/en/

### DB Lock

A master process running with a particular configuration assumes that it is the
only master process interacting with the database tables specified by the
configuration.

**There must never be more than one master process performing a run for the same
configuration**. Running two or more master processes might wipe out the
`files` table containing the checksums and meta data for all monitored files.

Read-only operations such as listing runs or checksum warnings may be performed
at any time, though they may in practice be locked out for most of the time
while a run is ongoing.

The master contains a database lock which is acquired and held while a master
process performs a run. Only one master process can hold the lock, thus multiple
accidentally started master processes are prevented from interfering with each
other.

### Workload

These are the primary operations performed by the master:

 * Insert all files into empty table `inserts`.
 * Table synchronisation through joins (`files` updated with `inserts` data):
   Update (join with `inserts` table), delete (all not-updated files), insert
   (join with `inserts` table).
 * Full `files` table random read. Supported by index on `rand` column. This
   happens with the same pace as checksum computation in small chunks.
 * Random reads of `files` and then updates. When a checksum is computed, the
   column from `files` is loaded to check for a checksum mismatch, then the new
   checksum is written back (along with meta data). This operation is batched
   for higher performance.

All operations have been crafted to only run for a small amount of time (~
seconds) and only touch a fixed amount of rows (this translates to row locks) in
a transaction, regardless of the total size of the data set. The amount of rows
per transaction can be configured through configuration options.

### Connecting

Only the master requires access to the MySQL server. The connection must be
configured in the configuration file through the `db.datasourcename` option.
The data source name is a string and must follow the
[specification][go-mysql-dsn] of the Golang MySQL driver in use.

[go-mysql-dsn]: https://github.com/go-sql-driver/mysql#dsn-data-source-name

### Likely Problems

 * Insufficient size of the buffer pool. This problem often materialises in the
   form of a "Error 1206: The total number of locks exceeds the lock table size"
   error.

   To mitigate this, either the buffer pool size may be increased (a few GiB are
   sufficient) or the size of the working set of transactions may be decreased
   through configuration options regarding the problematic component (see the
   documentation of configuration options below).

   Such errors most likely occur during the write back phase, when checksums are
   written back into the database. The total working set is determined by the
   `db.serverconcurrencyhint` times the
   `master.workqueue.writebacker.batcher.maxitems` times the
   `master.workqueue.writebacker.transactioner.maxtransactionsize` option.

 * Time-outs. Time-outs may be caused either by connection and transaction
   time-outs or by "waiting for lock" time-outs.

   Connection and transaction time-outs may be avoided by either increasing the
   time-out variables of MySQL or by decreasing maximum wait times and
   transaction sizes of the problematic omponent (see the documentation of
   configuration options below).

   Furthermore, time-outs may occur while a thread is waiting for locks to be
   released by another thread. This is indicative of erroneous application logic
   with regards to concurrency and concurrent database operations in particular.
   Re-starting the master may be a workaround when the issue rarely occurs.

 * Deadlocks. Some queries used by the master are run concurrently. There is a
   small probability that deadlocks occur. If this happens in production, the
   application logic should be revisited.

### Performance Concerns

The queries used by the master aim for a trade-off between performance and a
small working set of locked rows. By increasing the working set, performance can
be improved, however, generally this is only recommended after benchmarking.

The `db.serverconcurrencyhint` is the most important option for improving
performance. Operations supporting concurrent execution use this option to
determine the concurrency of execution. The recommended value is the number of
cores available on the system hosting the MySQL server. **This option is
recommended to be set.** Executing operations with increased concurrency can
lead to a significant speed-up (see `checksum_write_back` benchmark).

Performing operations concurrently increases the working set up to a factor of
`db.serverconcurrencyhint`. The default buffer pool size may have to be
increased to support such a working set. Otherwise, the master may experience
"Error 1206: The total number of locks exceeds the lock table size". A buffer
pool size of a few GiB should suffice for most production scenarios. The buffer
pool can be increased using the [`innodb_buffer_pool_size` system
variable][mariadb-buffer-pool-size]. When increasing the buffer pool size, it is
recommended to consider increasing `innodb_buffer_pool_instances` as well. The
MariaDB documentation contains a document about the [XtraDB/InnoDB Buffer
Pool][mariadb-buffer-pool] giving more details.

The MariaDB documentation contains an entire chapter dedicated to [Optimisation
and Tuning][mariadb-tuning]. The [Configuring MariaDB for Optimal
Performance][mariadb-optimal-performance] document describes the most important
tuning parameters. All available system variables (many of which can be
configured in the MariaDB configuration files) are documented in the [Server
System Variables document][mariadb-system-variables].

[mariadb-buffer-pool-size]: https://mariadb.com/kb/en/library/innodb-system-variables/#innodb_buffer_pool_size
[mariadb-buffer-pool]: https://mariadb.com/kb/en/library/xtradbinnodb-buffer-pool/
[mariadb-tuning]: https://mariadb.com/kb/en/library/optimization-and-tuning/
[mariadb-optimal-performance]: https://mariadb.com/kb/en/library/configuring-mariadb-for-optimal-performance/
[mariadb-system-variables]: https://mariadb.com/kb/en/library/server-system-variables/

## Redis

A [Redis][redis] server is required for master - worker communication. The
server must be reachable by the master and all workers. lsdf-checksum uses
[gocraft/worker][gocraft-work] as its work queue / job library.

lsdf-checksum uses a scheduler so that only a small number of jobs are queued in
the work queue at any time. Furthermore, each job represents a _pack_: I.e. a
job contains multiple files for which checksums are to be computed. Due to these
mechanisms, the load on Redis is rather low. Customising the configuration of
the Redis server is in all likelihood not required.

[redis]: https://redis.io/
[gocraft-work]: https://github.com/gocraft/work

## Master

### Checksum Mismatches

The master is responsible for recognising checksum mismatches. For this purpose
the `files` table is maintained by the master. The table contains a row for each
file with the file's name, last computed checksum and meta data about the file
at the time of checksum computation. Additionally, further bookkeeping details
are included.

During each run, a list of all monitored files is retrieved from the file system
including file meta data. Then, the SHA-1 checksum of the current file content
is computed for each file.

A checksum mismatch is presumed, when

 * a file has not changed according to its meta data in the file system (only
   the modification time is considered)
 * but its newly computed checksum differs from the checksum stored in the
   `files` table.

When a checksum mismatch is discovered, a checksum warning is emitted. This
consists of a new row being added to a `checksum_warnings` table and an `info`
level log message being written. The `checksum_warnings` table can be retrieved
using the `warnings` sub-command of the master binary.

The file content is read using the same snapshot which is used to retrieve the
file list. Hence, modifying a file in the live system while a checksum run is
ongoing does not interfere with the checksumming process.

### Run Mode

The master supports two run modes. The run mode is specified when first creating
the run.

 * Runs in `full` run mode follow the procedure explained in the "Checksum
   Mismatches" section. Checksums are computed for all monitored files and
   checksum warnings are emitted when mismatches are encountered.

 * Runs in `incremental` run mode are more lightweight. Checksums are only
   computed for files which have changed according to their meta data (only
   modification time is considered). Runs using this run mode are significantly
   faster and may be used to quickly compute missing checksums. However, this
   means that no checksum mismatches are discovered as only changed files are
   read.

### Performance

TBD

### CLI

The `lsdf-checksum-master` binary is provided to perform master operations.
Interactive documentation is available through the `--help` flag. The same
documentation can be printed in man format by using `--help-man`.

```
usage: lsdf-checksum-master [<flags>] <command> [<args> ...]

Master command of the lsdf-checksum system.

Flags:
  --help  Show context-sensitive help (also try --help-long and --help-man).

Commands:
  help [<command>...]
    Show help.

  abort --config=config.yaml [<flags>]
    Abort incomplete runs.

  complete --config=config.yaml [<flags>]
    Complete incomplete runs.

  initialise-run --config=config.yaml [<flags>]
    Initialise a new run but do not perform any processing. Specifically, the
    run execution stops as soon as its snapshot has been created in GPFS.

  run --config=config.yaml [<flags>]
    Perform a checksumming run.

  runs --config=config.yaml [<flags>]
    Print a list of runs.

  warnings --config=config.yaml [<flags>]
    Print a list of checksum warnings.
```

 * The `run` sub-command is the primary command. It creates a new run and
   performs all processing. `run` will not start unless all previous runs have
   been completed (or aborted).
 * `abort` and `complete` may be used to finish incomplete runs. `complete`
   continues processing at the point where run processing was interrupted.
   `abort` marks the run as aborted and performs clean-up but no processing
   (this is quick).
 * `runs` outputs a list of runs in either ASCII table or JSON format.
 * `warnings` outputs checksum warnings in either ASCII table or JSON format.
   Further flags are available to indicate the existence of warnings via the
   exit code.
 * `initialise-run` may be used to initialise a run to be completed later (via
   `complete`).

A running master process may be stopped at any time. The process responds to the
`SIGINT` (`2`) and `SIGTERM` (`15`) signals by attempting a clean, yet
instantaneous shut-down. **This is recommended.** If the process does not
completely shut-down, `SIGKILL` (`9`) should be used to kill the process.

### Configuration

All sub-commands of the master binary require a master configuration file.
The file must be in [YAML][yaml] format. This section documents all available
configuration options.

A minimal viable configuration looks like this:

```yaml
db:
  datasourcename: "root:@/test_schema"
master:
  filesystemname: gpfs2
  filesystemsubpath: "/emptyfiletree"
  redis:
    network: tcp
    address: ":6379"
```

[yaml]: https://yaml.org/

TBD

## Worker

### Performance

For controlling the performance and resource utilisation of a worker, the
primary configuration options are `worker.maxthroughput` and
`worker.concurrency`. **It is recommended to set these options.**

TBD

### CLI

```
usage: lsdf-checksum-worker [<flags>] <command> [<args> ...]

Worker command of the lsdf-checksum system.

Flags:
  --help  Show context-sensitive help (also try --help-long and --help-man).

Commands:
  help [<command>...]
    Show help.

  run --config=config.yaml [<flags>]
    Run the worker.
```

 * The `run` command starts the worker (in the foreground). The worker listens
   for jobs on the configured work queue.

A running worker process may be stopped at any time. The process responds to the
`SIGINT` (`2`) and `SIGTERM` (`15`) signals by attempting a clean, yet
instantaneous shut-down. **This is recommended.** If the process does not
completely shut-down, `SIGKILL` (`9`) should be used to kill the process.

### Configuration

The `run` sub-command of the worker binary requires a worker configuration file.
The file must be in [YAML][yaml] format. This section documents all available
configuration options.

A minimal viable configuration looks like this:

```yaml
worker:
  redis:
    network: tcp
    address: ":6379"
```

[yaml]: https://yaml.org/

TBD
