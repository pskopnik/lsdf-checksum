package workqueue

import (
	"time"
)

func (p *PerformanceMonitorConfig) CopyFrom(other *PerformanceMonitorConfig) {
	p.ProbeInterval = other.ProbeInterval

	p.MaxThroughput = other.MaxThroughput

	p.PauseQueueLength = other.PauseQueueLength
	p.ResumeQueueLength = other.ResumeQueueLength

	p.Workqueue = other.Workqueue
	p.Publisher = other.Publisher
	p.Logger = other.Logger
}

func (p *PerformanceMonitorConfig) Merge(other *PerformanceMonitorConfig) *PerformanceMonitorConfig {
	if other.ProbeInterval != time.Duration(0) {
		p.ProbeInterval = other.ProbeInterval
	}

	if other.MaxThroughput != 0 {
		p.MaxThroughput = other.MaxThroughput
	}

	if other.PauseQueueLength != 0 {
		p.PauseQueueLength = other.PauseQueueLength
	}
	if other.ResumeQueueLength != 0 {
		p.ResumeQueueLength = other.ResumeQueueLength
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
