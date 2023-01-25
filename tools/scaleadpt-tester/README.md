# scaleadpt-tester

This tool acts as a test for the `scaleadpt` package, which interacts with Spectrum Scale (formerly GPFS) filesystems.

All relevant methods of the `scaleadpt.Filesystem` are executed on a specific locally available Spectrum Scale filesystem and their output is compared to the user-specified spec.
Thus, it is checked whether the `scaleadpt` package works and gives the expected results on the filesystem.

See the `spec.example.yaml` file for documentation of the specification format.

```shell
$ go build .
$ ./scaleadpt-tester spec.yaml
```
