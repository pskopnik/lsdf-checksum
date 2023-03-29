// Package lengthsafe contains utilities to handle files with long files
// names.
//
// On Linux systems, there is a PATH_MAX constant (limits.h) as well as
// a pathconf option (_PC_PATH_MAX) which places an upper bound on path
// lengths passed to syscalls.
//
// This package allows opening files by dynamically creating temporary
// symlinks. These are used to create paths conforming to the limits.
// The most significant limitation of this approach is SYMLOOP_MAX, the number
// of symlinks which are traversed in the resolution of a pathname.
// _POSIX_SYMLOOP_MAX specifies the minimum acceptable value for POSIX systems,
// it is set to 8.
package lengthsafe

import (
	"io"
	"os"
	"path/filepath"

	"git.scc.kit.edu/sdm/lsdf-checksum/internal/osutils"
)

const lengthsafePrefix = "lsdf-checksum-lengthsafe-"

var _ io.ReadCloser = &LengthSafeFile{}
var _ io.WriteCloser = &LengthSafeFile{}

type LengthSafeFile struct {
	os.File

	inputName string
	openPath  string
	symlinks  []string
}

func (l *LengthSafeFile) Name() string {
	return l.inputName
}

func (l *LengthSafeFile) Close() error {
	err := l.File.Close()

	removeErr := l.removeSymlinks()
	if removeErr != nil && err == nil {
		err = removeErr
	}

	return err
}

func (l *LengthSafeFile) removeSymlinks() error {
	var firstErr, err error

	for i := len(l.symlinks) - 1; i >= 0; i-- {
		err = os.Remove(l.symlinks[i])
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func (l *LengthSafeFile) open(name string, osOpen func(string) (*os.File, error)) error {
	var err error
	l.inputName = name

	if uint(len(name)) > PathMax {
		err = ensureSymlinkMaxSet()
		if err != nil {
			return err
		}

		l.openPath, err = filepath.Abs(name)
		if err != nil {
			return err
		}

		if err != nil {
			return err
		}

		err = l.prepareSymlinks()
		if err != nil {
			return err
		}
	} else {
		l.openPath = name
	}

	f, err := osOpen(l.openPath)
	if err != nil {
		_ = l.removeSymlinks()
		return err
	}

	l.File = *f

	return nil
}

// prepareSymlinks creates the symlinks necessary to open openPath.
// After creation the openPath field is set to the new path using the symlink
// structure suitable for an open() syscall.
func (l *LengthSafeFile) prepareSymlinks() error {
	var dir, remainder string
	path := l.openPath

	for {
		dir, remainder = splitPathOnSymlinkLimit(path)
		if len(remainder) == 0 {
			break
		}

		symlinkPath, err := osutils.CreateTempSymlink(dir, lengthsafePrefix, "", "")
		if err != nil {
			_ = l.removeSymlinks()
			return err
		}

		l.symlinks = append(l.symlinks, symlinkPath)
		path = filepath.Join(symlinkPath, remainder)
	}

	l.openPath = path

	return nil
}

func Open(name string) (*LengthSafeFile, error) {
	f := &LengthSafeFile{}
	err := f.open(name, func(subName string) (*os.File, error) {
		return os.Open(subName)
	})
	if err != nil {
		return nil, err
	}

	return f, nil
}

func Create(name string) (*LengthSafeFile, error) {
	f := &LengthSafeFile{}
	err := f.open(name, func(subName string) (*os.File, error) {
		return os.Create(subName)
	})
	if err != nil {
		return nil, err
	}

	return f, nil
}

func OpenFile(name string, flag int, perm os.FileMode) (*LengthSafeFile, error) {
	f := &LengthSafeFile{}
	err := f.open(name, func(subName string) (*os.File, error) {
		return os.OpenFile(subName, flag, perm)
	})
	if err != nil {
		return nil, err
	}

	return f, nil
}

func splitPathOnSymlinkLimit(path string) (dir string, remainder string) {
	if uint(len(path)) <= symlinkMax {
		return path, ""
	}

	dir = filepath.Dir(path[:symlinkMax])

	if path[(symlinkMax-1)] == filepath.Separator {
		remainder = path[symlinkMax:]
	} else {
		remainder = filepath.Base(path[:symlinkMax]) + path[symlinkMax:]
	}

	return
}
