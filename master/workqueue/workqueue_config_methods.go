package workqueue

func (c *Config) CopyFrom(other *Config) {
	c.FileSystemName = other.FileSystemName
	c.RedisPrefix = other.RedisPrefix

	c.RunID = other.RunID
	c.SnapshotName = other.SnapshotName

	c.DB = other.DB
	c.Logger = other.Logger
	c.Pool = other.Pool

	c.Workqueue.CopyFrom(&other.Workqueue)
	c.EWMAController.CopyFrom(&other.EWMAController)
	c.Producer.CopyFrom(&other.Producer)
	c.QueueWatcher.CopyFrom(&other.QueueWatcher)
	c.WriteBacker.CopyFrom(&other.WriteBacker)
	c.PerformanceMonitor.CopyFrom(&other.PerformanceMonitor)
}

func (c *Config) Merge(other *Config) *Config {
	if len(other.RedisPrefix) > 0 {
		c.RedisPrefix = other.RedisPrefix
	}
	if len(other.FileSystemName) > 0 {
		c.FileSystemName = other.FileSystemName
	}

	if other.RunID != 0 {
		c.RunID = other.RunID
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
	if other.Pool != nil {
		c.Pool = other.Pool
	}

	c.Workqueue.Merge(&other.Workqueue)
	c.EWMAController.Merge(&other.EWMAController)
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
