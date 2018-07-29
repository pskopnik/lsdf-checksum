package workqueue

import (
	"errors"
)

var (
	stopSignalled error = errors.New("Stop signalled")
)

const (
	CalculateChecksumJobName string = "CalculateChecksum"

	writeBackJobNameBase string = "WriteBack"
)

func WriteBackJobName(fileSystemName, snapshotName string) string {
	return writeBackJobNameBase + "-" + fileSystemName + "-" + snapshotName
}

type WorkPackFile struct {
	Id   int
	Path string
}

type WorkPack struct {
	FileSystemName string
	SnapshotName   string
	Files          []WorkPackFile
}

type WriteBackPackFile struct {
	Id       int
	Checksum []byte
}

type WriteBackPack struct {
	Files []WriteBackPackFile
}
