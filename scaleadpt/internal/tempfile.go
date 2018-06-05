package internal

import (
	"os"
	"path/filepath"
)

const touchPermissions = 0666

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

// TouchNonExistingTempFile creates a new temporary file. The path of this
// file is then returned.
// prefix is prepended before a series of random characters in the file name,
// postfix is appended after the series of random characters.
func TouchNonExistingTempFile(prefix, postfix string) string {
	var name, path string
	var err error
	var f *os.File

	err = os.ErrExist
	for os.IsExist(err) {
		name = prefix + RandomString(10)
		path = filepath.Join(os.TempDir(), name+postfix)
		f, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, touchPermissions)
	}
	if err == nil {
		f.Close()
	}

	return path
}
