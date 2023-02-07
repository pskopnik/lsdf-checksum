# gentree

Generates file trees according to probability distributions.

Because the approach is recursive, there is no practical way of estimating the total size of the file tree generated.
Instead, perform several attempts with the dry-run option enabled (a new seed is generated every time), and choose a suitable tree according to the printed statistics.
Then, specify the same seed in the config file when performing the proper run (with dry-run disabled) generating files.

See the `config.example.yaml` file for the available options.

```shell
$ go build .
# Simulate generation by specifying dry_run: true in the config.
# Repeat (new seed generated every time) until the resulting file tree
# is reasonable in terms of total size, number of files and directories.
$ ./gentree config.yaml
$ ./gentree config.yaml
$ ./gentree config.yaml
# Actually generate and populate the file tree.
# Set dry_run: false in the config and add the seed from above.
$ ./gentree config.yaml
```
