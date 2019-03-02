package worker

func (c *Config) CopyFrom(other *Config) {
	c.MaxThroughput = other.MaxThroughput
	c.Concurrency = other.Concurrency

	c.Logger = other.Logger

	c.Redis.CopyFrom(&other.Redis)

	c.RedisPrefix = other.RedisPrefix
}

func (c *Config) Merge(other *Config) *Config {
	if other.MaxThroughput != 0 {
		c.MaxThroughput = other.MaxThroughput
	}
	if other.Concurrency != 0 {
		c.Concurrency = other.Concurrency
	}

	if other.Logger != nil {
		c.Logger = other.Logger
	}

	c.Redis.Merge(&other.Redis)

	if len(other.RedisPrefix) > 0 {
		c.RedisPrefix = other.RedisPrefix
	}

	return c
}

func (c *Config) Clone() *Config {
	config := &Config{}
	config.CopyFrom(c)
	return config
}
