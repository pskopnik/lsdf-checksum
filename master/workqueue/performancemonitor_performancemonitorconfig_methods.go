package workqueue

import (
	"time"
)

func (p *PerformanceMonitorConfig) CopyFrom(other *PerformanceMonitorConfig) {
	p.MaxThroughput = other.MaxThroughput
	p.CheckInterval = other.CheckInterval

	p.PauseQueueLength = other.PauseQueueLength
	p.ResumeQueueLength = other.ResumeQueueLength

	p.Workqueue = other.Workqueue
	p.Publisher = other.Publisher
	p.Logger = other.Logger
}

func (p *PerformanceMonitorConfig) Merge(other *PerformanceMonitorConfig) *PerformanceMonitorConfig {
	if other.MaxThroughput != 0 {
		p.MaxThroughput = other.MaxThroughput
	}
	if other.CheckInterval != time.Duration(0) {
		p.CheckInterval = other.CheckInterval
	}

	if other.MaxThroughput != 0 {
		p.MaxThroughput = other.MaxThroughput
	}
	if other.MaxThroughput != 0 {
		p.MaxThroughput = other.MaxThroughput
	}

	if other.Workqueue != nil {
		p.Workqueue = other.Workqueue
	}
	if other.Publisher != nil {
		p.Publisher = other.Publisher
	}
	if other.Logger != nil {
		p.Logger = other.Logger
	}

	return p
}

func (p *PerformanceMonitorConfig) Clone() *PerformanceMonitorConfig {
	config := &PerformanceMonitorConfig{}
	config.CopyFrom(p)
	return config
}
