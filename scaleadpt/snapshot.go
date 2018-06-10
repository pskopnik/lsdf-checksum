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

// SnapshotDirsInfo captures infos about snapshot directories of a filesystem.
// It contains all information in the output of the mmsnapdir command, see the
// reference for more details:
//
//    https://www.ibm.com/support/knowledgecenter/en/STXKQY_4.2.3/com.ibm.spectrum.scale.v4r23.doc/bl1adm_mmsnapdir.htm
type SnapshotDirsInfo struct {
	// Global is the name of the global snapshots subdirectory.
	Global string
	// GlobalsInFileset indicates whether global snapshots are available in the
	// root directory of all independent filesets.
	GlobalsInFileset bool
	// Fileset is the name of the fileset snapshots subdirectory.
	Fileset string
	// AllDirectories indicates whether snapshot directories are available in
	// all directories of the file system.
	// If it is false, then snapshot subdirectories are only available in the
	// root directory of independent filesets (including the root directory of
	// the file system).
	AllDirectories bool
}
