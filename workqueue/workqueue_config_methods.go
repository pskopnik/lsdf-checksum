package workqueue

func (c *Config) CopyFrom(other *Config) {
	c.Pool = other.Pool
	c.Prefix = other.Prefix
	c.Logger = other.Logger
	c.DConfigProbeInterval = other.DConfigProbeInterval
}

func (c *Config) Merge(other *Config) *Config {
	if other.Pool != nil {
		c.Pool = other.Pool
	}
	if len(other.Prefix) > 0 {
		c.Prefix = other.Prefix
	}
	if other.Logger != nil {
		c.Logger = other.Logger
	}
	if other.DConfigProbeInterval != 0 {
		c.DConfigProbeInterval = other.DConfigProbeInterval
	}

	return c
}

func (c *Config) Clone() *Config {
	config := &Config{}
	config.CopyFrom(c)
	return config
}
