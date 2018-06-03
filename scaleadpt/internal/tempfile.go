package internal

import (
	"os"
	"path/filepath"
)

// NonExistingTempFile constructs and returns the path to a non-existing
// temporary file.
// prefix is prepended before a series of random characters in the file name,
// postfix is appended after the series of random characters.
func NonExistingTempFile(prefix, postfix string) string {
	var name, path string
	var err error

	for !os.IsNotExist(err) {
		name = prefix + RandomString(10)
		path = filepath.Join(os.TempDir(), name+postfix)
		_, err = os.Stat(path)
	}

	return path
}
