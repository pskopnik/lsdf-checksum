package main

import (
	"time"
)

func (d *DBConfig) CopyFrom(other *DBConfig) {
	d.Driver = other.Driver
	d.DataSourceName = other.DataSourceName
	d.TablePrefix = other.TablePrefix
	d.MaxOpenConns = other.MaxOpenConns
	d.MaxIdleConns = other.MaxIdleConns
	d.ConnMaxLifetime = other.ConnMaxLifetime
	d.TablesNameBase.Files = other.TablesNameBase.Files
	d.TablesNameBase.FileData = other.TablesNameBase.FileData
}

func (d *DBConfig) Merge(other *DBConfig) *DBConfig {
	if len(other.Driver) > 0 {
		d.Driver = other.Driver
	}
	if len(other.DataSourceName) > 0 {
		d.DataSourceName = other.DataSourceName
	}
	if len(other.TablePrefix) > 0 {
		d.TablePrefix = other.TablePrefix
	}
	if other.MaxOpenConns != 0 {
		d.MaxOpenConns = other.MaxOpenConns
	}
	if other.MaxIdleConns != 0 {
		d.MaxIdleConns = other.MaxIdleConns
	}
	if other.ConnMaxLifetime != time.Duration(0) {
		d.ConnMaxLifetime = other.ConnMaxLifetime
	}
	if len(other.TablesNameBase.Files) > 0 {
		d.TablesNameBase.Files = other.TablesNameBase.Files
	}
	if len(other.TablesNameBase.FileData) > 0 {
		d.TablesNameBase.FileData = other.TablesNameBase.FileData
	}

	return d
}

func (d *DBConfig) Clone() *DBConfig {
	config := &DBConfig{}
	config.CopyFrom(d)
	return config
}
