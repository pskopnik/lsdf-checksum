package worker

func (c *Config) CopyFrom(other *Config) {
	c.Concurrency = other.Concurrency
	c.MaxThroughput = other.MaxThroughput
	c.FileReadSize = other.FileReadSize

	c.Logger = other.Logger

	c.Redis.CopyFrom(&other.Redis)
	c.RedisPrefix = other.RedisPrefix

	c.Workqueue.CopyFrom(&other.Workqueue)

	c.PrefixTTL = other.PrefixTTL
	c.PrefixerReapingInterval = other.PrefixerReapingInterval
	c.WorkqueueReapingInterval = other.WorkqueueReapingInterval
}

func (c *Config) Merge(other *Config) *Config {
	if other.Concurrency != 0 {
		c.Concurrency = other.Concurrency
	}
	if other.MaxThroughput != 0 {
		c.MaxThroughput = other.MaxThroughput
	}
	if other.FileReadSize != 0 {
		c.FileReadSize = other.FileReadSize
	}

	if other.Logger != nil {
		c.Logger = other.Logger
	}

	c.Redis.Merge(&other.Redis)
	if len(other.RedisPrefix) > 0 {
		c.RedisPrefix = other.RedisPrefix
	}

	c.Workqueue.Merge(&other.Workqueue)

	if other.PrefixTTL != 0 {
		c.PrefixTTL = other.PrefixTTL
	}
	if other.PrefixerReapingInterval != 0 {
		c.PrefixerReapingInterval = other.PrefixerReapingInterval
	}
	if other.WorkqueueReapingInterval != 0 {
		c.WorkqueueReapingInterval = other.WorkqueueReapingInterval
	}

	return c
}

func (c *Config) Clone() *Config {
	config := &Config{}
	config.CopyFrom(c)
	return config
}
