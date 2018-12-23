# Results

## Environment

The benchmarks were performed on the gpfstest-01 system running with no
significant load using MariaDB 10.3.10.

The system uses a default MariaDB configuration with the exception of a large
buffer pool (32 GiB). This large size is chosen to avoid exhaustion of lock
space (`innodb lock table exceeded`).

```
[mariadb]
innodb_buffer_pool_size = 34359738368
```

The test data set consists of `11959698` files. The files have been generated
by `gentree` tool. The maximum path length is `214`, the minimum path length
is `46` and the average path length is `210.9621`.

## Output

The output of the `bench.py bench-all` command is in the file
`bench_results.csv`. The benchmark took `8174m48.272s`.

A version without the analysis output (from `ANALYZE FORMAT=json`) is included
in the git repository as `bench_results_no_analysis.csv`.

## Evaluation

### "Update Existing" Operation

The most critical results are the timings of the `update_existing` operation
in the `no_inserts` scenario. The timings only show significant divergence
when the number of indices updated differs. The _Don't Update Rand_ family of
cases performs well as an index is defined on the `rand` field of the `files`
table.

The two most intuitive implementations of the _Don't Update Rand_ family have
approximately the same timings.

`no_inserts,TestCaseDontUpdateRand,update_existing,658.732077`
```json
{
  "query_block": {
    "select_id": 1,
    "r_loops": 1,
    "r_total_time_ms": 658939,
    "table": {
      "table_name": "inserts",
      "access_type": "ALL",
      "r_loops": 1,
      "rows": 11707466,
      "r_rows": 1.2e7,
      "r_total_time_ms": 27621,
      "filtered": 100,
      "r_filtered": 100,
      "attached_condition": "inserts.last_seen = 2"
    },
    "table": {
      "table_name": "files",
      "access_type": "ref",
      "possible_keys": ["path"],
      "key": "path",
      "key_length": "2050",
      "used_key_parts": ["path"],
      "ref": ["test_schema.inserts.path"],
      "r_loops": 11959698,
      "rows": 1,
      "r_rows": 1,
      "r_total_time_ms": 237747,
      "filtered": 100,
      "r_filtered": 100,
      "attached_condition": "files.path = inserts.path and files.last_seen <> 2"
    }
  }
}
```

`no_inserts,TestCaseDontUpdateRandUpdateAllConditionsInOn,update_existing,658.61696`
```json
{
  "query_block": {
    "select_id": 1,
    "r_loops": 1,
    "r_total_time_ms": 658833,
    "const_condition": "1",
    "table": {
      "table_name": "inserts",
      "access_type": "ALL",
      "r_loops": 1,
      "rows": 11717656,
      "r_rows": 1.2e7,
      "r_total_time_ms": 26868,
      "filtered": 100,
      "r_filtered": 100
    },
    "table": {
      "table_name": "files",
      "access_type": "ref",
      "possible_keys": ["path"],
      "key": "path",
      "key_length": "2050",
      "used_key_parts": ["path"],
      "ref": ["test_schema.inserts.path"],
      "r_loops": 11959698,
      "rows": 1,
      "r_rows": 1,
      "r_total_time_ms": 236046,
      "filtered": 100,
      "r_filtered": 100,
      "attached_condition": "trigcond(files.path = inserts.path and files.last_seen <> 2 and trigcond(inserts.last_seen = 2))"
    }
  }
}
```

Information about the `trigcond` function are sparse. The MySQL Reference
Manual mentions the function at least twice. However, the use of `trigcond`
does not seem to make much difference in the case at hand.

 * [MySQL Reference: Optimizing Subqueries with the EXISTS Strategy](https://dev.mysql.com/doc/refman/8.0/en/subquery-optimization-with-exists.html)
 * [MySQL Reference: Index Condition Pushdown Optimization](https://dev.mysql.com/doc/refman/8.0/en/index-condition-pushdown-optimization.html)

The timings for both queries indicate that about 265 seconds are spent on
retrieving records and about 395 seconds are spent updating these records.

Interestingly, when adding the `last_seen` field to a composite index
`"path_last_seen" = ["path", "last_seen"]` only the `path` part is used when
evaluating which records to update. This might be due to the low cardinality
of the index, the `last_seen` column of all rows is `1` when the query is
executed.

`no_inserts,TestCaseUpdateNoInsertsConditionsAllInOnCompositeIndex,update_existing,1414.060662`
```json
{
  "query_block": {
    "select_id": 1,
    "r_loops": 1,
    "r_total_time_ms": 1.41e6,
    "const_condition": "1",
    "table": {
      "table_name": "inserts",
      "access_type": "ALL",
      "r_loops": 1,
      "rows": 11785863,
      "r_rows": 1.2e7,
      "r_total_time_ms": 27049,
      "filtered": 100,
      "r_filtered": 100
    },
    "table": {
      "table_name": "files",
      "access_type": "ref",
      "possible_keys": ["path_last_seen"],
      "key": "path_last_seen",
      "key_length": "2050",
      "used_key_parts": ["path"],
      "ref": ["test_schema.inserts.path"],
      "r_loops": 11959698,
      "rows": 1,
      "r_rows": 1,
      "r_total_time_ms": 988155,
      "filtered": 100,
      "r_filtered": 100,
      "attached_condition": "trigcond(files.path = inserts.path and files.last_seen <> 2)"
    }
  }
}
```

In the `empty` scenario there are the cases can be partitioned into two sets,
each with similar timings within. The two sets show significantly different
timings. The table used in the outer loop of the `JOIN` execution causes the
difference (`files` is empty).

`empty,TestCaseUpdateNoInsertsConditions,update_existing,0.00123`
```json
{
  "query_block": {
    "select_id": 1,
    "r_loops": 1,
    "r_total_time_ms": 0.0301,
    "table": {
      "table_name": "files",
      "access_type": "ALL",
      "possible_keys": ["path"],
      "r_loops": 1,
      "rows": 1,
      "r_rows": 0,
      "r_total_time_ms": 0.0137,
      "filtered": 100,
      "r_filtered": 100,
      "attached_condition": "files.last_seen <> 1"
    },
    "table": {
      "table_name": "inserts",
      "access_type": "ALL",
      "r_loops": 0,
      "rows": 11832397,
      "r_rows": null,
      "filtered": 100,
      "r_filtered": null,
      "attached_condition": "inserts.path = files.path"
    }
  }
}
```

`empty,TestCaseUpdateNoInsertsConditionsAllInOn,update_existing,51.160124`
```json
{
  "query_block": {
    "select_id": 1,
    "r_loops": 1,
    "r_total_time_ms": 51154,
    "const_condition": "1",
    "table": {
      "table_name": "inserts",
      "access_type": "ALL",
      "r_loops": 1,
      "rows": 11383159,
      "r_rows": 1.2e7,
      "r_total_time_ms": 25362,
      "filtered": 100,
      "r_filtered": 100
    },
    "table": {
      "table_name": "files",
      "access_type": "ALL",
      "possible_keys": ["path"],
      "r_loops": 11959698,
      "rows": 1,
      "r_rows": 0,
      "r_total_time_ms": 8190.3,
      "filtered": 100,
      "r_filtered": 100,
      "attached_condition": "trigcond(files.path = inserts.path and files.last_seen <> 1)"
    }
  }
}
```

## "Insert New" Operation

Most cases perform similarly for the `insert_new` operation. Using an `EXISTS`
clause instead of a `JOIN` is slower in both scenarios.

In the `empty` scenario all files have to be newly inserted into the `files`
table. Here, a similar effects to the `update_existing` operation occurs: The
number of indices which have to be updated affects the execution time
significantly.

In the `no_inserts` scenario no files are inserted. The
`TestCaseInsertAllConditionsInOn` case stands out as being several times
slower than all other cases.

`no_inserts,TestCaseInsertAllConditionsInOn,insert_new,810.294801`
```json
{
  "query_block": {
    "select_id": 1,
    "r_loops": 1,
    "r_total_time_ms": 810123,
    "const_condition": "1",
    "temporary_table": {
      "table": {
        "table_name": "inserts",
        "access_type": "ALL",
        "r_loops": 1,
        "rows": 11684544,
        "r_rows": 1.2e7,
        "r_total_time_ms": 10101,
        "filtered": 100,
        "r_filtered": 100
      }
    }
  }
}
```

`no_inserts,TestCaseDontUpdateRand,insert_new,68.106538`
```json
{
  "query_block": {
    "select_id": 1,
    "r_loops": 1,
    "r_total_time_ms": 68164,
    "temporary_table": {
      "table": {
        "table_name": "inserts",
        "access_type": "ALL",
        "r_loops": 1,
        "rows": 11685412,
        "r_rows": 1.2e7,
        "r_total_time_ms": 9271,
        "filtered": 100,
        "r_filtered": 100,
        "attached_condition": "inserts.last_seen = 2"
      },
      "table": {
        "table_name": "files",
        "access_type": "ref",
        "possible_keys": ["path"],
        "key": "path",
        "key_length": "2050",
        "used_key_parts": ["path"],
        "ref": ["test_schema.inserts.path"],
        "r_loops": 11959698,
        "rows": 1,
        "r_rows": 1,
        "r_total_time_ms": 51656,
        "filtered": 100,
        "r_filtered": 100,
        "attached_condition": "trigcond(files.`id` is null) and trigcond(files.path = inserts.path)",
        "not_exists": true
      }
    }
  }
}
```

## "Delete Old" Operation

Most cases perform similarly for the `delete_old` operation in the
`no_inserts` scenario.

The `TestCaseDontUpdateRandUpdateNoInsertsConditionsAllInOnCompositeIndex`
case performs slightly better than all others. However, the analyze output of
the queries reveals that there is no difference in the query execution. Thus,
the difference probably stems from a difference in system load at execution
time.

`no_inserts,TestCaseInitial,delete_old,32.397247`
```json
{
  "query_block": {
    "select_id": 1,
    "r_total_time_ms": 32379,
    "table": {
      "delete": 1,
      "table_name": "files",
      "access_type": "ALL",
      "rows": 11699764,
      "r_rows": 1.2e7,
      "r_filtered": 0,
      "r_total_time_ms": 27554,
      "attached_condition": "files.last_seen <> 2"
    }
  }
}
```


`no_inserts,TestCaseDontUpdateRandUpdateNoInsertsConditionsAllInOnCompositeIndex,delete_old,28.837073`
```json
{
  "query_block": {
    "select_id": 1,
    "r_total_time_ms": 28818,
    "table": {
      "delete": 1,
      "table_name": "files",
      "access_type": "ALL",
      "rows": 11710688,
      "r_rows": 1.2e7,
      "r_filtered": 0,
      "r_total_time_ms": 24274,
      "attached_condition": "files.last_seen <> 2"
    }
  }
}
```
