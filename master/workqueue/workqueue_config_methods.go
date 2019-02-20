package workqueue

func (c *Config) CopyFrom(other *Config) {
	c.FileSystemName = other.FileSystemName

	c.RunId = other.RunId
	c.SnapshotName = other.SnapshotName

	c.DB = other.DB
	c.Logger = other.Logger

	c.Redis.CopyFrom(&other.Redis)
	c.EWMAScheduler.CopyFrom(&other.EWMAScheduler)
	c.Producer.CopyFrom(&other.Producer)
	c.QueueWatcher.CopyFrom(&other.QueueWatcher)
	c.WriteBacker.CopyFrom(&other.WriteBacker)
	c.PerformanceMonitor.CopyFrom(&other.PerformanceMonitor)
}

func (c *Config) Merge(other *Config) *Config {
	if len(other.FileSystemName) > 0 {
		c.FileSystemName = other.FileSystemName
	}

	if other.RunId != 0 {
		c.RunId = other.RunId
	}
	if len(other.SnapshotName) > 0 {
		c.SnapshotName = other.SnapshotName
	}

	if other.DB != nil {
		c.DB = other.DB
	}
	if other.Logger != nil {
		c.Logger = other.Logger
	}

	if other.Producer.MinWorkPackFileSize != 0 {
		c.Producer.MinWorkPackFileSize = other.Producer.MinWorkPackFileSize
	}
	if other.Producer.MaxWorkPackFileNumber != 0 {
		c.Producer.MaxWorkPackFileNumber = other.Producer.MaxWorkPackFileNumber
	}
	if other.Producer.FetchRowBatchSize != 0 {
		c.Producer.FetchRowBatchSize = other.Producer.FetchRowBatchSize
	}
	if other.Producer.RowBufferSize != 0 {
		c.Producer.RowBufferSize = other.Producer.RowBufferSize
	}

	if len(other.Producer.FileSystemName) > 0 {
		c.Producer.FileSystemName = other.Producer.FileSystemName
	}
	if len(other.Producer.Namespace) > 0 {
		c.Producer.Namespace = other.Producer.Namespace
	}

	if len(other.Producer.SnapshotName) > 0 {
		c.Producer.SnapshotName = other.Producer.SnapshotName
	}

	if other.Producer.Pool != nil {
		c.Producer.Pool = other.Producer.Pool
	}
	if other.Producer.DB != nil {
		c.Producer.DB = other.Producer.DB
	}
	if other.Producer.Logger != nil {
		c.Producer.Logger = other.Producer.Logger
	}

	if other.Producer.Controller != nil {
		c.Producer.Controller = other.Producer.Controller
	}

	c.Redis.Merge(&other.Redis)
	c.EWMAScheduler.Merge(&other.EWMAScheduler)
	c.Producer.Merge(&other.Producer)
	c.QueueWatcher.Merge(&other.QueueWatcher)
	c.WriteBacker.Merge(&other.WriteBacker)
	c.PerformanceMonitor.Merge(&other.PerformanceMonitor)

	return c
}

func (c *Config) Clone() *Config {
	config := &Config{}
	config.CopyFrom(c)
	return config
}
