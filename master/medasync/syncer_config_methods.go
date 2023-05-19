package medasync

func (c *Config) CopyFrom(other *Config) {
	c.SynchronisationChunkSize = other.SynchronisationChunkSize
	c.Inserter.CopyFrom(&other.Inserter)

	c.Subpath = other.Subpath
	c.TemporaryDirectory = other.TemporaryDirectory
	c.GlobalWorkDirectory = other.GlobalWorkDirectory
	c.NodeList = other.NodeList
	c.ExcludePathPatterns = other.ExcludePathPatterns

	c.SnapshotName = other.SnapshotName
	c.RunID = other.RunID
	c.SyncMode = other.SyncMode

	c.DB = other.DB
	c.FileSystem = other.FileSystem
	c.Logger = other.Logger
}

func (c *Config) Merge(other *Config) *Config {
	if other.SynchronisationChunkSize != 0 {
		c.SynchronisationChunkSize = other.SynchronisationChunkSize
	}
	c.Inserter.Merge(&other.Inserter)

	if len(other.Subpath) > 0 {
		c.Subpath = other.Subpath
	}
	if len(other.TemporaryDirectory) > 0 {
		c.TemporaryDirectory = other.TemporaryDirectory
	}
	if len(other.GlobalWorkDirectory) > 0 {
		c.GlobalWorkDirectory = other.GlobalWorkDirectory
	}
	if len(other.NodeList) > 0 {
		c.NodeList = other.NodeList
	}
	if len(other.ExcludePathPatterns) > 0 {
		c.ExcludePathPatterns = other.ExcludePathPatterns
	}

	if len(other.SnapshotName) > 0 {
		c.SnapshotName = other.SnapshotName
	}
	if other.RunID != 0 {
		c.RunID = other.RunID
	}
	if other.SyncMode != 0 {
		c.SyncMode = other.SyncMode
	}

	if other.DB != nil {
		c.DB = other.DB
	}
	if other.FileSystem != nil {
		c.FileSystem = other.FileSystem
	}
	if other.Logger != nil {
		c.Logger = other.Logger
	}

	return c
}

func (c *Config) Clone() *Config {
	config := &Config{}
	config.CopyFrom(c)
	return config
}
