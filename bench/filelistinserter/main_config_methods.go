package main

func (c *Config) CopyFrom(other *Config) {
	c.DB.CopyFrom(&other.DB)
	c.FileListPath = other.FileListPath
	c.MaxTransactionSize = other.MaxTransactionSize
	c.StopAfterNRows = other.StopAfterNRows
}

func (c *Config) Merge(other *Config) *Config {
	c.DB.Merge(&other.DB)

	if len(other.FileListPath) > 0 {
		c.FileListPath = other.FileListPath
	}

	if other.MaxTransactionSize != 0 {
		c.MaxTransactionSize = other.MaxTransactionSize
	}

	if other.StopAfterNRows != 0 {
		c.StopAfterNRows = other.StopAfterNRows
	}

	return c
}

func (c *Config) Clone() *Config {
	config := &Config{}
	config.CopyFrom(c)
	return config
}
