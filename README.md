# lsdf-checksum

lsdf-checksum is a distributed system to compute checksums of file content in
large-scale file systems with the goal of verifying file data integrity.

lsdf-checksum uses [gocraft/work][gocraft-work] with a [LedisDB][ledisdb]
backend as a queue system and stores all its data in a MySQL / MariaDB database.
It operates on [IBM Spectrum Scale][spectrum-scale] file systems.

The project was initially developed as part of a practical course at the
Steinbuch Centre of Computing (SCC) at the Karlsruhe Institute of Technology
(KIT). [Most slides][slides] of the presentation concluding the course are
available (the slides may be difficult to understand without the corresponding
presentation, however).

[gocraft-work]: https://github.com/gocraft/work
[ledisdb]: http://ledisdb.com/
[spectrum-scale]: https://en.wikipedia.org/wiki/IBM_Spectrum_Scale
[slides]: https://buckets.meta.mailsrv.io/share/X6MP1mDqwCckSZfzHDLeENZ8OIeeyUUe/slides.pdf

## Motivation

By increasing the total size of storage systems, the rate of error (bits per
time) is also increased. This problem has been discussed in the literature, for
example _Rosenthal, David SH. "Keeping bits safe: how hard can it be?."
Communications of the ACM 53.11 (2010): 47-55_.

lsdf-checksum has been designed to be used within the several large file systems
operated by the SCC, especially the [Large Scale Data Facility (LSDF)][lsdf] and
[GridKa][gridka]. The goal is to regularly compute and store checksums for each
file in the file system. If a file has not been changed by a user since the last
run and yet the checksum has changed, a warning is issued. The system must be
run regularly, so that it is still possible to restore a file from a backup.

The file systems are powered by [IBM Spectrum Scale][spectrum-scale] and
lsdf-checksum uses snapshots to work on a static version of the file system
during each run. IBM Spectrum Scale includes a policy engine, which is used to
compile a list of all files including some meta-data in the file system.

[lsdf]: https://www.scc.kit.edu/en/research/11843.php
[gridka]: http://www.gridka.de/welcome-en.html

## Building

The lsdf-checksum project has two primary commands:

 * `lsdf-checksum-master` is the master component of the system. This command
   contains the functionality for managing and performing checksum runs. It
   also allows querying the meta data database for checksum mismatches.
 * `lsdf-checksum-worker` is the light-weight worker component of the system.
   Workers receive work packs containing files to be checksummed. After
   reading the files, their checksums are send back to the master.

The binaries are built using a recent go version (tested with go1.12). Execute
the following commands in the root folder of this repository. Go will fetch
all dependencies. The output are the two binaries in the current working
directory.

```bash
go build ./cmd/lsdf-checksum-master
go build ./cmd/lsdf-checksum-worker
```

Both binaries do not depend on significant runtime libraries (e.g. libc is
required). Both binaries contain help texts (`--help`). Calling the binaries
only with the `--help-man` flag outputs a man page for the command.

## Development Status

This repository contains the prototype / work-in-progress of the lsdf-checksum
project. Most code is functional and has already been used at scale. However,
the code has not reached production-y stability yet. Hence, there are no
buildable binaries included in this repository.

Some open issues:

 * Operator binaries and interface documentation.
 * Optimisations of the meta database synchronisation queries.
 * Review of error handling.
 * Review of configuration passing (`...Config` types).
 * Embedding of LedisDB instead of requiring an external Redis server.
 * Optimisations of the checksum write-back procedure. It should be possible to
   use HDDs to store the database.

Development on this project is due to resume in December 2018.
