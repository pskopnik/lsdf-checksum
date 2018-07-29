package workqueue

import (
	"time"
)

func (w *WriteBackerConfig) CopyFrom(other *WriteBackerConfig) {
	w.Batch = other.Batch

	w.FileSystemName = other.FileSystemName
	w.Namespace = other.Namespace

	w.RunId = other.RunId
	w.SnapshotName = other.SnapshotName

	w.Pool = other.Pool
	w.DB = other.DB
	w.Logger = other.Logger
}

func (w *WriteBackerConfig) Merge(other *WriteBackerConfig) *WriteBackerConfig {
	if other.Batch.MinTime != time.Duration(0) {
		w.Batch.MinTime = other.Batch.MinTime
	}
	if other.Batch.MinItems != 0 {
		w.Batch.MinItems = other.Batch.MinItems
	}
	if other.Batch.MaxTime != time.Duration(0) {
		w.Batch.MaxTime = other.Batch.MaxTime
	}
	if other.Batch.MaxItems != 0 {
		w.Batch.MaxItems = other.Batch.MaxItems
	}

	if len(other.FileSystemName) > 0 {
		w.FileSystemName = other.FileSystemName
	}
	if len(other.Namespace) > 0 {
		w.Namespace = other.Namespace
	}

	if other.RunId != 0 {
		w.RunId = other.RunId
	}
	if len(other.SnapshotName) > 0 {
		w.SnapshotName = other.SnapshotName
	}

	if other.Pool != nil {
		w.Pool = other.Pool
	}
	if other.DB != nil {
		w.DB = other.DB
	}
	if other.Logger != nil {
		w.Logger = other.Logger
	}

	return w
}

func (w *WriteBackerConfig) Clone() *WriteBackerConfig {
	config := &WriteBackerConfig{}
	config.CopyFrom(w)
	return config
}
