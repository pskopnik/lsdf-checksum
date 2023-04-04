package main

func (c *Config) CopyFrom(other *Config) {
	c.DB.CopyFrom(&other.DB)
	c.FileListPath = other.FileListPath
	c.Inserter.CopyFrom(&other.Inserter)
	c.StopAfterNRows = other.StopAfterNRows
	c.CPUProfile = other.CPUProfile
}

func (c *Config) Merge(other *Config) *Config {
	c.DB.Merge(&other.DB)

	if len(other.FileListPath) > 0 {
		c.FileListPath = other.FileListPath
	}

	c.Inserter.Merge(&other.Inserter)

	if other.StopAfterNRows != 0 {
		c.StopAfterNRows = other.StopAfterNRows
	}

	if len(other.CPUProfile) > 0 {
		c.CPUProfile = other.CPUProfile
	}

	return c
}

func (c *Config) Clone() *Config {
	config := &Config{}
	config.CopyFrom(c)
	return config
}
