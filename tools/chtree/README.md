# chtree

Walks fully through a file tree and probabilistically changes files.
By resetting the modification time (mtime) inode/stat field, corruptions can be simulated.

For each file, the tool will decide whether to apply configuration-specified "changers".
The given `likelihood` is evaluated for each changer individually to make the decision whether the changer will be executed on the file.
When multiple changers are performed on a file, it is expected that the modification time is updated every time.
With coarse granularity (e.g. second) this may not happen.

The tool writes a log of changed files (i.e. executed changers) to stdout while writing (few) other log lines to stderr.

See the `config.example.yaml` file for the available options.

```shell
$ go build .
# Change the tree, appending the JSONL change log output to a file.
$ ./chtree config.yaml >> changes.jsonl
```

## JSON Change Log

The log of changed files is written in [JSON Lines](https://jsonlines.org/) format.
Each log message is a single line which contains a JSON value, specifically a JSON object.
The encoded object does not contain any newlines (`\n`).

This is the format of each change file message (formatted here with newlines for legibility):

```json
{
	"event": "changed",
	"path": "/some/path/inside/root_dir/file01",
	"corrupted": false,
	"corrupted_only": false,
	"changers": [{
		"method": "append",
		"id": "#0"
	}]
}
```

 * `event`: `"changed"` means a file was changed, i.e. its content modified.
 * `path`: Absolute path to the file.
 * `corrupted`: Did at least one executed changer reset the mtime to simulate a corruption?
   Likely `corrupted_only` should be used rather than `corrupted`, because another changer may have triggered an mtime update.
 * `corrupted_only`: Did all executed changers reset the mtime to simulate a corruption?
 * `changers`: List of changers which were executed on the file.
   `method` is the name of the method of the changer.
   `id` is the ID, as specified in the config file or automatically generated.
