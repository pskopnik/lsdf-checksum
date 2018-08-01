package workqueue

import (
	"context"

	"github.com/gomodule/redigo/redis"
	"github.com/imdario/mergo"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"gopkg.in/tomb.v2"

	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

//go:generate confions config ProducerConfig

type ProducerConfig struct {
	MinWorkPackFileSize uint64

	FileSystemName string
	Namespace      string

	SnapshotName string

	Pool   *redis.Pool
	DB     *sqlx.DB
	Logger logrus.FieldLogger

	Controller SchedulingController
}

var ProducerDefaultConfig = &ProducerConfig{
	MinWorkPackFileSize: 5 * 1024 * 1024, // 5 MiB
}

type Producer struct {
	Config *ProducerConfig

	tomb *tomb.Tomb

	filesRows      *sqlx.Rows
	queueScheduler *QueueScheduler
	fieldLogger    logrus.FieldLogger
}

func NewProducer(config *ProducerConfig) *Producer {
	return &Producer{
		Config: config,
	}
}

func (p *Producer) Start(ctx context.Context) {
	p.tomb, _ = tomb.WithContext(ctx)

	p.fieldLogger = p.Config.Logger.WithFields(logrus.Fields{
		"snapshot":   p.Config.SnapshotName,
		"filesystem": p.Config.FileSystemName,
		"namespace":  p.Config.Namespace,
		"package":    "workqueue",
		"component":  "Producer",
	})

	queueSchedulerConfig := &QueueSchedulerConfig{
		Namespace:  p.Config.Namespace,
		JobName:    CalculateChecksumJobName,
		Pool:       p.Config.Pool,
		Logger:     p.Config.Logger,
		Controller: p.Config.Controller,
	}

	p.queueScheduler = NewQueueScheduler(queueSchedulerConfig)

	p.tomb.Go(func() error {
		p.tomb.Go(p.run)

		p.queueScheduler.Start(p.tomb.Context(nil))

		p.tomb.Go(p.queueSchedulerWaiter)

		return nil
	})
}

func (p *Producer) SignalStop() {
	p.tomb.Kill(stopSignalled)
}

func (p *Producer) Wait() {
	<-p.tomb.Dead()
}

func (p *Producer) Dead() <-chan struct{} {
	return p.tomb.Dead()
}

func (p *Producer) Err() error {
	return p.tomb.Err()
}

func (p *Producer) run() error {
	var err error
	var exhausted bool

	p.filesRows, err = meda.FilesQueryCtxFilesToBeRead(
		p.tomb.Context(nil),
		p.Config.DB,
	)
	if err != nil {
		p.fieldLogger.WithError(err).WithFields(logrus.Fields{
			"action": "stopping",
		}).Error("Encountered error during query setup")

		return err
	}

	c := p.queueScheduler.C()

	p.fieldLogger.Info("Starting listening to production requests")

L:
	for {
		select {
		case productionRequest, ok := <-c:
			if !ok {
				break L
			}
			p.fieldLogger.WithField("n", productionRequest.N).Debug("Received production request")
			exhausted, err = p.produce(productionRequest.N)
			if err != nil || exhausted {
				break L
			}
		case <-p.tomb.Dying():
			break L
		}
	}

	_ = p.filesRows.Close()

	if err != nil {
		p.fieldLogger.WithError(err).WithFields(logrus.Fields{
			"action":    "stopping",
			"exhausted": exhausted,
		}).Error("Encountered error while producing")
	} else {
		p.fieldLogger.WithFields(logrus.Fields{
			"action":    "stopping",
			"exhausted": exhausted,
		}).Info("Finished listening to production requests")
	}

	p.queueScheduler.SignalStop()

	return err
}

func (p *Producer) produce(n uint) (bool, error) {
	var err error
	file := meda.File{}
	workPack := WorkPack{
		FileSystemName: p.Config.FileSystemName,
		SnapshotName:   p.Config.SnapshotName,
		Files:          make([]WorkPackFile, 0, 1),
	}
	workPackMap := make(map[string]interface{})
	var totalFileSize uint64 = 0
	exhausted := false

	for i := uint(0); i < n && !exhausted; i++ {
		// Initialise work pack
		workPack.Files = workPack.Files[:0]
		totalFileSize = 0

		// Prepare work pack
		for totalFileSize < p.Config.MinWorkPackFileSize {
			// Retrieve next file from database
			if !p.filesRows.Next() {
				exhausted = true
				break
			}

			err = p.filesRows.StructScan(&file)
			if err != nil {
				return exhausted, err
			}

			workPack.Files = append(workPack.Files, WorkPackFile{
				Id:   file.Id,
				Path: file.Path,
			})

			totalFileSize += file.FileSize
		}

		err = p.enqueue(&workPack, workPackMap)
		if err != nil {
			return exhausted, err
		}
	}

	return exhausted, nil
}

// enqueue enqueues a WorkPack using the Producer's queueScheduler's Enqueue
// method.
// The second parameter, workPackMap, may be passed optionally to avoid
// reallocating a map on every call.
func (p *Producer) enqueue(workPack *WorkPack, workPackMap map[string]interface{}) error {
	var err error

	if workPackMap == nil {
		workPackMap = make(map[string]interface{})
	}

	if len(workPack.Files) == 0 {
		return nil
	}

	err = mergo.Map(&workPackMap, workPack, mergo.WithOverride)
	if err != nil {
		return err
	}

	// Enqueue work pack
	_, err = p.queueScheduler.Enqueue(workPackMap)
	if err != nil {
		return err
	}

	return nil
}

func (p *Producer) queueSchedulerWaiter() error {
	p.queueScheduler.Wait()

	return nil
}
