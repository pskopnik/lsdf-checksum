# dbfilescanner

This tool reads the files table in the database once.
It can be used to efficiently read the entire table into the InnoDB buffer pool (in-memory cache).
It also serves as a basis to run custom sequential analysis on the data.

As its first and only argument it expects a configuration file like for the lsdf-checksum-master.
Only the `db` stanza from the config is used.

```shell
$ go build .
$ ./dbfilescanner config.yaml
```
