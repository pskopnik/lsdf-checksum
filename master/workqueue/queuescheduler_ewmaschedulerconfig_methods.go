package workqueue

import (
	"time"
)

func (e *EWMASchedulerConfig) CopyFrom(other *EWMASchedulerConfig) {
	e.ConsumptionLifetime = other.ConsumptionLifetime

	e.MinThreshold = other.MinThreshold
	e.MinWorkerThreshold = other.MinWorkerThreshold

	e.StartUpSteps = other.StartUpSteps
	e.StartUpInterval = other.StartUpInterval
	e.StartUpWorkerTheshold = other.StartUpWorkerTheshold

	e.MaintainingInterval = other.MaintainingInterval
	e.MaintainingDeviationFactor = other.MaintainingDeviationFactor
	e.MaintainingDeviationAlpha = other.MaintainingDeviationAlpha
}

func (e *EWMASchedulerConfig) Merge(other *EWMASchedulerConfig) *EWMASchedulerConfig {
	if other.ConsumptionLifetime != time.Duration(0) {
		e.ConsumptionLifetime = other.ConsumptionLifetime
	}

	if other.MinThreshold != 0 {
		e.MinThreshold = other.MinThreshold
	}
	if other.MinWorkerThreshold != 0 {
		e.MinWorkerThreshold = other.MinWorkerThreshold
	}

	if other.StartUpSteps != 0 {
		e.StartUpSteps = other.StartUpSteps
	}
	if other.StartUpInterval != time.Duration(0) {
		e.StartUpInterval = other.StartUpInterval
	}
	if other.StartUpWorkerTheshold != 0 {
		e.StartUpWorkerTheshold = other.StartUpWorkerTheshold
	}

	if other.MaintainingInterval != time.Duration(0) {
		e.MaintainingInterval = other.MaintainingInterval
	}
	if other.MaintainingDeviationFactor != 0 {
		e.MaintainingDeviationFactor = other.MaintainingDeviationFactor
	}
	if other.MaintainingDeviationAlpha != 0 {
		e.MaintainingDeviationAlpha = other.MaintainingDeviationAlpha
	}

	return e
}

func (e *EWMASchedulerConfig) Clone() *EWMASchedulerConfig {
	config := &EWMASchedulerConfig{}
	config.CopyFrom(e)
	return config
}
