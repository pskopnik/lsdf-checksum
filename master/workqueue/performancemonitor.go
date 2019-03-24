package workqueue

import (
	"context"
	"time"

	"github.com/apex/log"
	"github.com/gomodule/redigo/redis"
	"gopkg.in/tomb.v2"

	"git.scc.kit.edu/sdm/lsdf-checksum/internal/lifecycle"
)

const (
	performanceNamespaceBase string = "lsdf-checksum/workqueue:performance"
	maxNodeThroughputSubkey  string = "max_node_throughput"
)

func MaxNodeThroughputKey(prefix, unit string) string {
	return prefix + performanceNamespaceBase + ":" + unit + ":" + maxNodeThroughputSubkey
}

type GetNodesNumer interface {
	GetNodesNum() (uint, error)
}

//go:generate confions config PerformanceMonitorConfig

type PerformanceMonitorConfig struct {
	MaxThroughput uint64
	CheckInterval time.Duration
	Prefix        string

	Unit string

	Pool          *redis.Pool   `yaml:"-"`
	Logger        log.Interface `yaml:"-"`
	GetNodesNumer GetNodesNumer `yaml:"-"`
}

var PerformanceMonitorDefaultConfig = &PerformanceMonitorConfig{
	CheckInterval: 5 * time.Second,
}

type PerformanceMonitor struct {
	Config *PerformanceMonitorConfig

	tomb *tomb.Tomb

	lastMaxNodeThroughput uint64

	fieldLogger log.Interface
}

func NewPerformanceMonitor(config *PerformanceMonitorConfig) *PerformanceMonitor {
	return &PerformanceMonitor{
		Config: config,
	}
}

func (p *PerformanceMonitor) Start(ctx context.Context) {
	p.fieldLogger = p.Config.Logger.WithFields(log.Fields{
		"unit":      p.Config.Unit,
		"prefix":    p.Config.Prefix,
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

	err = p.computeAll()
	if err != nil {
		p.fieldLogger.WithError(err).WithFields(log.Fields{
			"action": "stopping",
		}).Error("Encountered error while computing performance constraints")

		return err
	}

	p.fieldLogger.Info("Starting monitoring loop")

	// Exhaust timer
	if !timer.Stop() {
		<-timer.C
	}
L:
	for {
		timer.Reset(p.Config.CheckInterval)

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

	return nil
}

func (p *PerformanceMonitor) computeMaxNodeThroughput() error {
	nodesNum, err := p.Config.GetNodesNumer.GetNodesNum()
	if err != nil {
		p.fieldLogger.WithError(err).WithFields(log.Fields{
			"action": "skipping",
		}).Warn("Encountered error while fetching number of nodes")

		return nil
	}

	maxNodeThroughput := p.Config.MaxThroughput / uint64(nodesNum)
	if maxNodeThroughput == p.lastMaxNodeThroughput {
		return nil
	}

	key := MaxNodeThroughputKey(p.Config.Prefix, p.Config.Unit)

	conn := p.Config.Pool.Get()
	defer conn.Close()

	_, err = conn.Do("SET", key, maxNodeThroughput)
	if err != nil {
		p.fieldLogger.WithError(err).WithFields(log.Fields{
			"action":              "escalating",
			"key":                 key,
			"max_node_throughput": maxNodeThroughput,
		}).Error("Encountered error while setting max_node_throughput in the database")

		return err
	}
	p.lastMaxNodeThroughput = maxNodeThroughput

	return nil
}
