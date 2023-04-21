package scaleadpt

import (
	"time"

	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt/options"
)

// SnapshotOpt provides simple access to SnapshotOptioners methods.
var SnapshotOpt = options.SnapshotOptioners{}

type Snapshot struct {
	filesystem *FileSystem

	ID        int
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
//	https://www.ibm.com/support/knowledgecenter/en/STXKQY_4.2.3/com.ibm.spectrum.scale.v4r23.doc/bl1adm_mmsnapdir.htm
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
