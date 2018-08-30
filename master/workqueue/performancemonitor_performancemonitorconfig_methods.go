package workqueue

import (
	"time"
)

func (p *PerformanceMonitorConfig) CopyFrom(other *PerformanceMonitorConfig) {
	p.MaxThroughput = other.MaxThroughput
	p.CheckInterval = other.CheckInterval
	p.Prefix = other.Prefix

	p.Unit = other.Unit

	p.Pool = other.Pool
	p.Logger = other.Logger
	p.GetNodesNumer = other.GetNodesNumer
}

func (p *PerformanceMonitorConfig) Merge(other *PerformanceMonitorConfig) *PerformanceMonitorConfig {
	if other.MaxThroughput != 0 {
		p.MaxThroughput = other.MaxThroughput
	}
	if other.CheckInterval != time.Duration(0) {
		p.CheckInterval = other.CheckInterval
	}
	if len(other.Prefix) > 0 {
		p.Prefix = other.Prefix
	}

	if len(other.Unit) > 0 {
		p.Unit = other.Unit
	}

	if other.Pool != nil {
		p.Pool = other.Pool
	}
	if other.Logger != nil {
		p.Logger = other.Logger
	}
	if other.GetNodesNumer != nil {
		p.GetNodesNumer = other.GetNodesNumer
	}

	return p
}

func (p *PerformanceMonitorConfig) Clone() *PerformanceMonitorConfig {
	config := &PerformanceMonitorConfig{}
	config.CopyFrom(p)
	return config
}
