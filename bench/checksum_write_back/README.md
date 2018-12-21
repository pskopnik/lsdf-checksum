# Checksum Write Back Benchmarking

This benchmarks contains implementation for different methods of writing back
calculated checksums to the meta data database. The benchmark measures the
time each methods takes with different parameters. Each method also checks for
checksum mismatches. In reaction to a checksum mismatch either a log entry is
written or a new row is added to the _Checksum Warnings_ table.

The parameters considered are the following. Not all methods support all
parameters.

 * Batch Size: number of rows affected per query
 * Transaction Size: number of queries per transaction
 * Concurrency: number of concurrent workers

## Requirements

 * Go for compiling the benchmark executable.
 * MySQL server.
 * A table with _File Data_. All rows in this table will be used during
    benchmarking.

See `bench/metadata_db_syncing` for information on how to generate a File Data
table.

Running `go build .` in this directory creates the `checksum_write_back`
executable.

## Benchmarks

Running `checksum_write_back` with a configuration file as its
first and only command line parameter performs the benchmarks. Timing data is
written to stdout in CSV format. Log entries may be written to stderr.

### Methods

Each methods performs essentially the same operation:

 1. A number of pairs (file id, calculated checksum) are received.
 2. For each pair, the file is loaded from the meta data database.
 3. It is evaluated whether a checksum mismatch occurred. A checksum mismatch
     occurs when the file hasn't changed according to its meta data, but the
     calculated checksum is not equal to the checksum in the meta data
     database.
 4. The calculated checksum is written to the database. Furthermore, the
     columns `to_be_read` and `to_be_compared` are set to 0 and `last_read` is
     set to the current run id.
