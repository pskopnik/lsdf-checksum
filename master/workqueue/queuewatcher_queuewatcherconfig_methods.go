package workqueue

import (
	"time"
)

func (q *QueueWatcherConfig) CopyFrom(other *QueueWatcherConfig) {
	q.CheckInterval = other.CheckInterval
	q.MaxRetryJobChecks = other.MaxRetryJobChecks

	q.FileSystemName = other.FileSystemName
	q.Namespace = other.Namespace

	q.RunID = other.RunID
	q.SnapshotName = other.SnapshotName

	q.Pool = other.Pool
	q.Logger = other.Logger

	q.ProductionExhausted = other.ProductionExhausted
}

func (q *QueueWatcherConfig) Merge(other *QueueWatcherConfig) *QueueWatcherConfig {
	if other.CheckInterval != time.Duration(0) {
		q.CheckInterval = other.CheckInterval
	}
	if other.MaxRetryJobChecks != 0 {
		q.MaxRetryJobChecks = other.MaxRetryJobChecks
	}

	if len(other.FileSystemName) > 0 {
		q.FileSystemName = other.FileSystemName
	}
	if len(other.Namespace) > 0 {
		q.Namespace = other.Namespace
	}

	if other.RunID != 0 {
		q.RunID = other.RunID
	}
	if len(other.SnapshotName) > 0 {
		q.SnapshotName = other.SnapshotName
	}

	if other.Pool != nil {
		q.Pool = other.Pool
	}
	if other.Logger != nil {
		q.Logger = other.Logger
	}

	if other.ProductionExhausted != nil {
		q.ProductionExhausted = other.ProductionExhausted
	}

	return q
}

func (q *QueueWatcherConfig) Clone() *QueueWatcherConfig {
	config := &QueueWatcherConfig{}
	config.CopyFrom(q)
	return config
}
