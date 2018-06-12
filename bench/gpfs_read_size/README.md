# GPFS Read Size Benchmark

This benchmark tests different buffer sizes used while reading from a GPFS
file. The buffer is used as input to the `read` syscall.

## Preparation

The only preparation required is to create a suitably sized test file at
`/gpfs2/largetestfile`. For example:

```bash
dd if=/dev/urandom of=/gpfs2/largetestfile bs=8k count=$((10*1024*1024/8))
```
