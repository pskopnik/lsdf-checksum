# Pack Benchmark

This benchmark tests different methods of encoding work packs in the Args map of
gocraft/work [Job](https://godoc.org/github.com/gocraft/work#Job) struct.

The subdirectories contain a basic implementation as well as a benchmark to
evaluate its performance.

The implementations consist of a `main` function which simulates functionality
carried out by gocraft/work and calls two functions containing the
implementation of the encoding methods:

 * `propagateJob(jobArgs map[string]interface{}, workPack *workqueue.WorkPack) error`
    This function encodes `workPack` in the `jobArgs` map.
 * `retrievePack(jobArgs map[string]interface{}) (*workqueue.WorkPack, error)`
    This function retrieves and returns a `WorkPack` from the `jobArgs` map.

## json

The [JSON](https://www.json.org/) implementation uses
[`encoding/json`](https://golang.org/pkg/encoding/json/) to marshal the
`WorkPack` struct into a string. This string is then stored in the `pack` key of
the `jobsArgs` map.

## msgp

Here [MessagePack](https://msgpack.org/) is used to marshal the `WorkPack`
struct into a bytes slice. The [`msgp`](https://github.com/tinylib/msgp)
golang implementation is used. `msgp` utilises static code generation to achieve
high performance. The resulting byte slice is then encoded as a base64 string
(powered by [encoding/base64](https://golang.org/pkg/encoding/base64/) from the
golang standard library and the `RawStdEncoding`) and this string is stored in
the `pack` key of the `jobArgs` map.

## mergo

This implementation attempts to use [`mergo`](https://github.com/imdario/mergo)
to map the `WorkMap` into the `jobArgs` map (gocraft/work encodes the map using
JSON). However, the mapping back from the `jobArgs` map to the `WorkPack` struct
proved difficult and the implementation is not functional.
