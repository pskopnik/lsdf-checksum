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

//go:generate msgp

type WorkPackFile struct {
	Id   uint64
	Path string
}

type WorkPack struct {
	FileSystemName string
	SnapshotName   string
	Files          []WorkPackFile
}

type WriteBackPackFile struct {
	Id       uint64
	Checksum []byte
}

type WriteBackPack struct {
	Files []WriteBackPackFile
}
