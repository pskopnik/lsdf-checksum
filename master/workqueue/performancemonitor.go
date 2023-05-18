package workqueue

import (
	"context"
	"fmt"
	"time"

	"github.com/apex/log"
	"gopkg.in/tomb.v2"

	"git.scc.kit.edu/sdm/lsdf-checksum/internal/lifecycle"
	"git.scc.kit.edu/sdm/lsdf-checksum/workqueue"
)

//go:generate confions config PerformanceMonitorConfig

type PerformanceMonitorConfig struct {
	ProbeInterval time.Duration

	MaxThroughput uint64

	PauseQueueLength  int
	ResumeQueueLength int

	Workqueue *workqueue.Workqueue        `yaml:"-"`
	Publisher *workqueue.DConfigPublisher `yaml:"-"`
	Logger    log.Interface               `yaml:"-"`
}

var PerformanceMonitorDefaultConfig = &PerformanceMonitorConfig{
	ProbeInterval: 5 * time.Second,

	PauseQueueLength:  10000,
	ResumeQueueLength: 1000,
}

type PerformanceMonitor struct {
	Config *PerformanceMonitorConfig

	tomb *tomb.Tomb

	isPaused bool

	fieldLogger log.Interface
}

func NewPerformanceMonitor(config *PerformanceMonitorConfig) *PerformanceMonitor {
	return &PerformanceMonitor{
		Config: config,
	}
}

func (p *PerformanceMonitor) Start(ctx context.Context) {
	p.fieldLogger = p.Config.Logger.WithFields(log.Fields{
		"component": "workqueue.PerformanceMonitor",
	})

	p.tomb, _ = tomb.WithContext(ctx)

	p.tomb.Go(p.run)
}

func (p *PerformanceMonitor) SignalStop() {
	p.tomb.Kill(lifecycle.ErrStopSignalled)
}

func (p *PerformanceMonitor) Wait() error {
	return p.tomb.Wait()
}

func (p *PerformanceMonitor) Dead() <-chan struct{} {
	return p.tomb.Dead()
}

func (p *PerformanceMonitor) Err() error {
	return p.tomb.Err()
}

func (p *PerformanceMonitor) run() error {
	var err error
	dying := p.tomb.Dying()
	timer := time.NewTimer(time.Duration(0))

	p.fieldLogger.Info("Computing initial performance constraints")

	// If queue is still paused from last run, unpause now
	err = p.unpauseQueueIfPaused()
	if err != nil {
		p.fieldLogger.WithError(err).WithFields(log.Fields{
			"action": "stopping",
		}).Error("Encountered error while unpausing queue initially")

		return err
	}

	err = p.computeAll()
	if err != nil {
		p.fieldLogger.WithError(err).WithFields(log.Fields{
			"action": "stopping",
		}).Error("Encountered error while computing performance constraints initially")

		return err
	}

	p.fieldLogger.Info("Starting monitoring loop")

	// Exhaust timer
	if !timer.Stop() {
		<-timer.C
	}
L:
	for {
		timer.Reset(p.Config.ProbeInterval)

		select {
		case <-timer.C:
			p.fieldLogger.Debug("Re-computing performance constraints")
			err = p.computeAll()
			if err != nil {
				p.fieldLogger.WithError(err).WithFields(log.Fields{
					"action": "stopping",
				}).Error("Encountered error while computing performance constraints")

				return err
			}
		case <-dying:
			// Exhaust timer
			if !timer.Stop() {
				<-timer.C
			}

			break L
		}
	}

	p.fieldLogger.WithField("action", "stopping").Info("Finished monitoring loop")

	return nil
}

func (p *PerformanceMonitor) computeAll() error {
	var err error

	if p.Config.MaxThroughput > 0 {
		err = p.computeMaxNodeThroughput()
		if err != nil {
			return err
		}
	}

	if p.Config.PauseQueueLength > 0 {
		err = p.pauseQueueForBackpressure()
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *PerformanceMonitor) computeMaxNodeThroughput() error {
	info, err := p.Config.Workqueue.Queues().ComputeChecksum().GetWorkerInfo()
	if err != nil {
		p.fieldLogger.WithError(err).WithFields(log.Fields{
			"action": "skipping",
		}).Warn("Encountered error while fetching number of nodes")

		return nil
	}

	// TODO: Should this be spread across nodes or workers (threads)?
	var maxNodeThroughput uint64
	if info.NodeNum > 0 {
		maxNodeThroughput = p.Config.MaxThroughput / uint64(info.NodeNum)
	} else {
		// NodeNum == 0. Assume 1 node
		maxNodeThroughput = p.Config.MaxThroughput
	}

	_, err = p.Config.Publisher.MutatePublishData(func(d *workqueue.DConfigData) {
		if d.MaxNodeThroughput != maxNodeThroughput {
			p.fieldLogger.WithError(err).WithFields(log.Fields{
				"old_max_node_throughput": d.MaxNodeThroughput,
				"new_max_node_throughput": maxNodeThroughput,
			}).Debug("Updating max_node_throughput performance constraint")
			d.MaxNodeThroughput = maxNodeThroughput
		}
	})
	if err != nil {
		return err
	}

	return nil
}

func (p *PerformanceMonitor) pauseQueueForBackpressure() error {
	info, err := p.Config.Workqueue.Queues().WriteBack().GetQueueInfo()
	if err != nil {
		p.fieldLogger.WithError(err).WithFields(log.Fields{
			"action": "skipping",
		}).Warn("Encountered error while fetching queue info")

		return nil
	}

	fieldLogger := p.fieldLogger.WithFields(log.Fields{
		"pause_queue_length":  p.Config.PauseQueueLength,
		"resume_queue_length": p.Config.ResumeQueueLength,
		"queue_length":        info.QueuedJobs,
		"is_paused":           p.isPaused,
	})

	if p.isPaused {
		if info.QueuedJobs < uint64(p.Config.ResumeQueueLength) {
			fieldLogger.Debug("Unpausing queue for backpressure")
			err := p.Config.Workqueue.Queues().ComputeChecksum().Unpause()
			if err != nil {
				return fmt.Errorf("pauseQueueForBackpressure: unpausing queue: %w", err)
			}
			p.isPaused = false
		}
	} else {
		if info.QueuedJobs > uint64(p.Config.PauseQueueLength) {
			fieldLogger.Debug("Pausing queue for backpressure")
			err := p.Config.Workqueue.Queues().ComputeChecksum().Pause()
			if err != nil {
				return fmt.Errorf("pauseQueueForBackpressure: pausing queue: %w", err)
			}
			p.isPaused = true
		}
	}

	return nil
}

func (p *PerformanceMonitor) unpauseQueueIfPaused() error {
	isPaused, err := p.Config.Workqueue.Queues().ComputeChecksum().IsPaused()
	if err != nil {
		return fmt.Errorf("unpauseQueueIfPaused: querying queue pause state: %w", err)
	}

	if isPaused {
		err := p.Config.Workqueue.Queues().ComputeChecksum().Unpause()
		p.fieldLogger.Debug("Unpausing paused queue")
		if err != nil {
			return fmt.Errorf("unpauseQueueIfPaused: unpausing queue: %w", err)
		}
	}

	return nil
}
