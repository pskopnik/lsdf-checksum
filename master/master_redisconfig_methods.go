package master

import (
	"time"
)

func (r *RedisConfig) CopyFrom(other *RedisConfig) {
	r.Dialect = other.Dialect
	r.Network = other.Network
	r.Address = other.Address
	r.Database = other.Database
	r.Password = other.Password
	r.MaxIdle = other.MaxIdle
	r.IdleTimeout = other.IdleTimeout
	r.InternalMaxActive = other.InternalMaxActive
}

func (r *RedisConfig) Merge(other *RedisConfig) *RedisConfig {
	if len(other.Dialect) > 0 {
		r.Dialect = other.Dialect
	}
	if len(other.Network) > 0 {
		r.Network = other.Network
	}
	if len(other.Address) > 0 {
		r.Address = other.Address
	}
	if other.Database != 0 {
		r.Database = other.Database
	}
	if len(other.Password) > 0 {
		r.Password = other.Password
	}
	if other.MaxIdle != 0 {
		r.MaxIdle = other.MaxIdle
	}
	if other.IdleTimeout != time.Duration(0) {
		r.IdleTimeout = other.IdleTimeout
	}
	if other.InternalMaxActive != 0 {
		r.InternalMaxActive = other.InternalMaxActive
	}

	return r
}

func (r *RedisConfig) Clone() *RedisConfig {
	config := &RedisConfig{}
	config.CopyFrom(r)
	return config
}
