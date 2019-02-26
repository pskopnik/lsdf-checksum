package master

import (
	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

func (c *Config) CopyFrom(other *Config) {
	c.FileSystemName = other.FileSystemName
	c.FileSystemSubpath = other.FileSystemSubpath

	c.Run = other.Run
	c.TargetState = other.TargetState

	c.Logger = other.Logger
	c.DB = other.DB

	c.Redis.CopyFrom(&other.Redis)
	c.MedaSync.CopyFrom(&other.MedaSync)
	c.WorkQueue.CopyFrom(&other.WorkQueue)
}

func (c *Config) Merge(other *Config) *Config {
	if len(other.FileSystemName) > 0 {
		c.FileSystemName = other.FileSystemName
	}
	if len(other.FileSystemSubpath) > 0 {
		c.FileSystemSubpath = other.FileSystemSubpath
	}

	if other.Run != nil {
		c.Run = other.Run
	}
	if other.TargetState != meda.RSNil {
		c.TargetState = other.TargetState
	}

	if other.Logger != nil {
		c.Logger = other.Logger
	}
	if other.DB != nil {
		c.DB = other.DB
	}

	c.Redis.Merge(&other.Redis)
	c.MedaSync.Merge(&other.MedaSync)
	c.WorkQueue.Merge(&other.WorkQueue)

	return c
}

func (c *Config) Clone() *Config {
	config := &Config{}
	config.CopyFrom(c)
	return config
}
