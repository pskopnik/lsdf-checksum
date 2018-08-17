package worker

import (
	"time"
)

func (c *Config) CopyFrom(other *Config) {
	c.MaxThroughput = other.MaxThroughput
	c.Concurrency = other.Concurrency

	c.Logger = other.Logger

	c.Redis.Network = other.Redis.Network
	c.Redis.Address = other.Redis.Address
	c.Redis.Database = other.Redis.Database
	c.Redis.Prefix = other.Redis.Prefix
	c.Redis.Password = other.Redis.Password
	c.Redis.MaxIdle = other.Redis.MaxIdle
	c.Redis.IdleTimeout = other.Redis.IdleTimeout
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

	if len(other.Redis.Network) > 0 {
		c.Redis.Network = other.Redis.Network
	}
	if len(other.Redis.Address) > 0 {
		c.Redis.Address = other.Redis.Address
	}
	if other.Redis.Database != 0 {
		c.Redis.Database = other.Redis.Database
	}
	if len(other.Redis.Prefix) > 0 {
		c.Redis.Prefix = other.Redis.Prefix
	}
	if len(other.Redis.Password) > 0 {
		c.Redis.Password = other.Redis.Password
	}
	if other.Redis.MaxIdle != 0 {
		c.Redis.MaxIdle = other.Redis.MaxIdle
	}
	if other.Redis.IdleTimeout != time.Duration(0) {
		c.Redis.IdleTimeout = other.Redis.IdleTimeout
	}

	return c
}

func (c *Config) Clone() *Config {
	config := &Config{}
	config.CopyFrom(c)
	return config
}
