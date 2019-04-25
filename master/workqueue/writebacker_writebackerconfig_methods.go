package workqueue

func (w *WriteBackerConfig) CopyFrom(other *WriteBackerConfig) {
	w.Batcher.MaxItems = other.Batcher.MaxItems
	w.Batcher.MaxWaitTime = other.Batcher.MaxWaitTime
	w.Batcher.BatchBufferSize = other.Batcher.BatchBufferSize

	w.Transactioner.DB = other.Transactioner.DB
	w.Transactioner.MaxTransactionSize = other.Transactioner.MaxTransactionSize
	w.Transactioner.MaxTransactionLifetime = other.Transactioner.MaxTransactionLifetime

	w.FileSystemName = other.FileSystemName
	w.Namespace = other.Namespace

	w.RunID = other.RunID
	w.SnapshotName = other.SnapshotName

	w.Pool = other.Pool
	w.DB = other.DB
	w.Logger = other.Logger
}

func (w *WriteBackerConfig) Merge(other *WriteBackerConfig) *WriteBackerConfig {
	if other.Batcher.MaxItems != 0 {
		w.Batcher.MaxItems = other.Batcher.MaxItems
	}
	if other.Batcher.MaxWaitTime != 0 {
		w.Batcher.MaxWaitTime = other.Batcher.MaxWaitTime
	}
	if other.Batcher.BatchBufferSize != 0 {
		w.Batcher.BatchBufferSize = other.Batcher.BatchBufferSize
	}

	if other.Transactioner.DB != nil {
		w.Transactioner.DB = other.Transactioner.DB
	}
	if other.Transactioner.MaxTransactionSize != 0 {
		w.Transactioner.MaxTransactionSize = other.Transactioner.MaxTransactionSize
	}
	if other.Transactioner.MaxTransactionLifetime != 0 {
		w.Transactioner.MaxTransactionLifetime = other.Transactioner.MaxTransactionLifetime
	}

	if len(other.FileSystemName) > 0 {
		w.FileSystemName = other.FileSystemName
	}
	if len(other.Namespace) > 0 {
		w.Namespace = other.Namespace
	}

	if other.RunID != 0 {
		w.RunID = other.RunID
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
