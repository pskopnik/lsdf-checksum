# Filelist Inserter Benchmark

This benchmark consists of an executable running the `meda.InsertsInserter` with configurable parameters.
It imports the InsertsInserter directly from the code base rather than containing its own copy, hence it is subject to the public interface and must be updated if the interface changes.
A filelist in the `scaleadpt/filelist` format serves as the input file.

## Core Program

The `filelistinserter` program reads its configuration from a YAML file and then inserts filelist (or its `n` first lines) into a database.
For all available options, see `config.example.yaml`.

Data is read from a filelist file produced by the GPFS policy engine.
There is no trivial way to produce one right now, but either the `scaleadpt/filelist.ApplyPolicy()` can be used, or the policy file is copied from the source code of the function.

```console
$ go build .
# Run with a configuration file
$ ./filelistinserter config.yaml
```

## Simple Benchmarking Suite

The bash (!) script `bench.sh` is a simple benchmarking suite running `filelistinserter` with different configuration options and collecting the execution times in CSV format.
See its source code to change the combination of configuration options.

```console
$ ./bench.sh > results.csv
```
