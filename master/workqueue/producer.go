package workqueue

import (
	"context"

	"github.com/apex/log"
	"github.com/gomodule/redigo/redis"
	"gopkg.in/tomb.v2"

	"git.scc.kit.edu/sdm/lsdf-checksum/internal/lifecycle"
	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
	"git.scc.kit.edu/sdm/lsdf-checksum/workqueue"
	"git.scc.kit.edu/sdm/lsdf-checksum/workqueue/scheduler"
)

//go:generate confions config ProducerConfig

type ProducerConfig struct {
	MinWorkPackFileSize   uint64
	MaxWorkPackFileNumber uint64
	FetchRowChunkSize     uint64
	FetchRowBatchSize     uint64

	FileSystemName string
	Namespace      string

	SnapshotName string

	Pool   *redis.Pool   `yaml:"-"`
	DB     *meda.DB      `yaml:"-"`
	Logger log.Interface `yaml:"-"`

	Controller scheduler.Controller `yaml:"-"`
}

var ProducerDefaultConfig = &ProducerConfig{
	MinWorkPackFileSize:   5 * 1024 * 1024, // 5 MiB
	MaxWorkPackFileNumber: 1000,
	FetchRowChunkSize:     50000,
	FetchRowBatchSize:     1000,
}

type Producer struct {
	Config *ProducerConfig

	tomb *tomb.Tomb
	ctx context.Context

	filesChan   chan meda.File
	lastRand    float64
	lastID      uint64
	scheduler   *scheduler.Scheduler
	fieldLogger log.Interface
}

func NewProducer(config *ProducerConfig) *Producer {
	return &Producer{
		Config: config,
	}
}

func (p *Producer) Start(ctx context.Context) {
	p.tomb, p.ctx = tomb.WithContext(ctx)

	p.fieldLogger = p.Config.Logger.WithFields(log.Fields{
		"snapshot":   p.Config.SnapshotName,
		"filesystem": p.Config.FileSystemName,
		"namespace":  p.Config.Namespace,
		"component":  "workqueue.Producer",
	})

	schedulerConfig := &scheduler.Config{
		Namespace:  p.Config.Namespace,
		JobName:    workqueue.CalculateChecksumJobName,
		Pool:       p.Config.Pool,
		Logger:     p.Config.Logger,
		Controller: p.Config.Controller,
	}

	p.lastRand = -1
	p.lastID = 0
	p.filesChan = make(chan meda.File, p.Config.FetchRowBatchSize)

	p.scheduler = scheduler.New(schedulerConfig)

	p.tomb.Go(func() error {
		p.tomb.Go(p.rowFetcher)

		p.tomb.Go(p.run)

		p.scheduler.Start(p.tomb.Context(nil))

		p.tomb.Go(p.schedulerWaiter)

		return nil
	})
}

func (p *Producer) SignalStop() {
	p.tomb.Kill(lifecycle.ErrStopSignalled)
}

func (p *Producer) Wait() error {
	return p.tomb.Wait()
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
	var order scheduler.ProductionOrder

	p.fieldLogger.Info("Starting listening for production orders")

	for {
		order, err = p.scheduler.AcquireOrder(p.ctx, uint(p.Config.FetchRowBatchSize))
		if err != nil {
			break
		}

		p.fieldLogger.WithField("n", order.Total()).Debug("Received production order")

		exhausted, err = p.fulfill(&order)
		if err != nil || exhausted {
			break
		}
	}

	if err != nil {
		p.fieldLogger.WithError(err).WithFields(log.Fields{
			"action":    "stopping",
			"exhausted": exhausted,
		}).Error("Encountered error while producing")
	} else {
		p.fieldLogger.WithFields(log.Fields{
			"action":    "stopping",
			"exhausted": exhausted,
		}).Info("Finished listening for production orders")
	}

	p.scheduler.SignalStop()

	return err
}

func (p *Producer) schedulerWaiter() error {
	p.scheduler.Wait()

	return nil
}

func (p *Producer) rowFetcher() error {
	var err error
	var exhausted bool

	ctx := p.tomb.Context(nil)
	dying := p.tomb.Dying()
	files := make([]meda.File, 0, p.Config.FetchRowBatchSize)
	filesFetcher := p.Config.DB.FilesToBeReadFetcher(&meda.FilesToBeReadFetcherConfig{
		ChunkSize: p.Config.FetchRowChunkSize,
	})

	defer close(p.filesChan)

	for !exhausted {
		files, err = filesFetcher.AppendNext(files[:0], ctx, nil, p.Config.FetchRowBatchSize)
		if err != nil {
			p.fieldLogger.WithError(err).WithFields(log.Fields{
				"action": "stopping",
			}).Error("Encountered error while fetching next batch of File rows")

			return err
		}
		exhausted = len(files) < int(p.Config.FetchRowBatchSize)

		p.fieldLogger.WithFields(log.Fields{
			"exhausted": exhausted,
			"count":     len(files),
		}).Debug("Fetched files from database")

		for _, file := range files {
			select {
			// If dying is closed, one of the two cases will be randomly (!)
			// selected. Thus, the function may not return instantly.
			case p.filesChan <- file:
			case <-dying:
				return tomb.ErrDying
			}
		}
	}

	return nil
}

func (p *Producer) fulfill(order *scheduler.ProductionOrder) (bool, error) {
	var err error
	file := meda.File{}
	workPack := workqueue.WorkPack{
		FileSystemName: p.Config.FileSystemName,
		SnapshotName:   p.Config.SnapshotName,
		Files:          make([]workqueue.WorkPackFile, 0, 16),
	}
	var exhausted, ok bool

	for i := 0; i < order.Total() && !exhausted; i++ {
		// Initialise work pack
		workPack.Files = workPack.Files[:0]
		var totalFileSize, numberOfFiles uint64

		// Prepare work pack
		for totalFileSize < p.Config.MinWorkPackFileSize && numberOfFiles < p.Config.MaxWorkPackFileNumber {
			// If the Producer is dying, filesChan is closed by rowFetcher().
			// Thus, this loop will also shut down quickly.
			file, ok = <-p.filesChan
			exhausted = !ok
			if exhausted {
				break
			}

			workPack.Files = append(workPack.Files, workqueue.WorkPackFile{
				ID:   file.ID,
				Path: file.Path,
			})

			totalFileSize += file.FileSize
			numberOfFiles++
		}

		_, err = order.Enqueue(&workPack)
		if err != nil {
			return exhausted, err
		}
	}

	return exhausted, nil
}
