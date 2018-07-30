# Results

This file contains results of the benchmarks performed for the different
encoding methods.

The result: `msgp` provides shorter on wire length as well as shorter execution
time than `json`.

## json

Benchmark results (`go test -bench=.`):

```
BenchmarkPropagateJob-4   	 1000000	      2120 ns/op
BenchmarkRetrievePack-4   	  200000	      6182 ns/op
```

Length of the "on wire" (gocraft/work) JSON message (`sampleWorkPack`):
**160 bytes**

## msgp

Benchmark results (`go test -bench=.`):

```
BenchmarkPropagateJob-4   	 2000000	       609 ns/op
BenchmarkRetrievePack-4   	 1000000	      1177 ns/op
```

Length of the "on wire" (gocraft/work) JSON message (`sampleWorkPack`):
**145 bytes**

## mergo

The implementation is not functional.
