package scaleadpt

import (
	"time"
)

type SnapshotOptioner interface {
	apply(*snapshotOptions)
}

type snapshotOptionerFunc func(*snapshotOptions)

func (s snapshotOptionerFunc) apply(options *snapshotOptions) {
	s(options)
}

type SnapshotOptions struct{}

func (_ SnapshotOptions) FileSet(fileset string) SnapshotOptioner {
	return snapshotOptionerFunc(func(options *snapshotOptions) {
		options.Fileset = fileset
	})
}

// SnapshotOpt provides simple access to SnapshotOptions methods.
var SnapshotOpt = SnapshotOptions{}

// snapshotOptions is the option aggregate structure for options for snapshot
// create operations.
type snapshotOptions struct {
	Fileset string
}

type Snapshot struct {
	filesystem *FileSystem

	Id        int
	Name      string
	Status    string
	CreatedAt time.Time
	Fileset   string
}

func (s *Snapshot) Delete() error {
	return s.filesystem.DeleteSnapshot(s.Name)
}
