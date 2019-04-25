package meda

func (c *Config) CopyFrom(other *Config) {
	c.Driver = other.Driver
	c.DataSourceName = other.DataSourceName
	c.TablePrefix = other.TablePrefix
	c.MaxOpenConns = other.MaxOpenConns
	c.MaxIdleConns = other.MaxIdleConns
	c.ConnMaxLifetime = other.ConnMaxLifetime
	c.LockKeepAliveInterval = other.LockKeepAliveInterval
	c.ServerConcurrencyHint = other.ServerConcurrencyHint
}

func (c *Config) Merge(other *Config) *Config {
	if len(other.Driver) > 0 {
		c.Driver = other.Driver
	}
	if len(other.DataSourceName) > 0 {
		c.DataSourceName = other.DataSourceName
	}
	if len(other.TablePrefix) > 0 {
		c.TablePrefix = other.TablePrefix
	}
	if other.MaxOpenConns != 0 {
		c.MaxOpenConns = other.MaxOpenConns
	}
	if other.MaxIdleConns != 0 {
		c.MaxIdleConns = other.MaxIdleConns
	}
	if other.ConnMaxLifetime != 0 {
		c.ConnMaxLifetime = other.ConnMaxLifetime
	}
	if other.LockKeepAliveInterval != 0 {
		c.LockKeepAliveInterval = other.LockKeepAliveInterval
	}
	if other.ServerConcurrencyHint != 0 {
		c.ServerConcurrencyHint = other.ServerConcurrencyHint
	}

	return c
}

func (c *Config) Clone() *Config {
	config := &Config{}
	config.CopyFrom(c)
	return config
}
