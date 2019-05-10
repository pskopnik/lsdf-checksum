package workqueue

import (
	"context"

	"github.com/apex/log"
	"github.com/gomodule/redigo/redis"
	"gopkg.in/tomb.v2"

	"git.scc.kit.edu/sdm/lsdf-checksum/internal/lifecycle"
	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
	"git.scc.kit.edu/sdm/lsdf-checksum/workqueue"
)

//go:generate confions config ProducerConfig

type ProducerConfig struct {
	MinWorkPackFileSize   uint64
	MaxWorkPackFileNumber uint64
	FetchRowChunkSize     uint64
	FetchRowBatchSize     uint64
	RowBufferSize         uint64

	FileSystemName string
	Namespace      string

	SnapshotName string

	Pool   *redis.Pool   `yaml:"-"`
	DB     *meda.DB      `yaml:"-"`
	Logger log.Interface `yaml:"-"`

	Controller SchedulingController `yaml:"-"`
}

var ProducerDefaultConfig = &ProducerConfig{
	MinWorkPackFileSize:   5 * 1024 * 1024, // 5 MiB
	MaxWorkPackFileNumber: 1000,
	FetchRowChunkSize:     50000,
	FetchRowBatchSize:     1000,
	RowBufferSize:         1000,
}

type Producer struct {
	Config *ProducerConfig

	tomb *tomb.Tomb

	filesChan      chan meda.File
	lastRand       float64
	lastID         uint64
	queueScheduler *QueueScheduler
	fieldLogger    log.Interface
}

func NewProducer(config *ProducerConfig) *Producer {
	return &Producer{
		Config: config,
	}
}

func (p *Producer) Start(ctx context.Context) {
	p.tomb, _ = tomb.WithContext(ctx)

	p.fieldLogger = p.Config.Logger.WithFields(log.Fields{
		"snapshot":   p.Config.SnapshotName,
		"filesystem": p.Config.FileSystemName,
		"namespace":  p.Config.Namespace,
		"component":  "workqueue.Producer",
	})

	queueSchedulerConfig := &QueueSchedulerConfig{
		Namespace:  p.Config.Namespace,
		JobName:    workqueue.CalculateChecksumJobName,
		Pool:       p.Config.Pool,
		Logger:     p.Config.Logger,
		Controller: p.Config.Controller,
	}

	p.lastRand = -1
	p.lastID = 0
	p.filesChan = make(chan meda.File, p.Config.RowBufferSize)

	p.queueScheduler = NewQueueScheduler(queueSchedulerConfig)

	p.tomb.Go(func() error {
		p.tomb.Go(p.rowFetcher)

		p.tomb.Go(p.run)

		p.queueScheduler.Start(p.tomb.Context(nil))

		p.tomb.Go(p.queueSchedulerWaiter)

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

	c := p.queueScheduler.C()
	dying := p.tomb.Dying()

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
		case <-dying:
			break L
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
		}).Info("Finished listening to production requests")
	}

	p.queueScheduler.SignalStop()

	return err
}

func (p *Producer) queueSchedulerWaiter() error {
	p.queueScheduler.Wait()

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
		exhausted = len(files) != cap(files)

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

func (p *Producer) produce(n uint) (bool, error) {
	var err error
	file := meda.File{}
	workPack := workqueue.WorkPack{
		FileSystemName: p.Config.FileSystemName,
		SnapshotName:   p.Config.SnapshotName,
		Files:          make([]workqueue.WorkPackFile, 0, 1),
	}
	workPackMap := make(map[string]interface{})
	var totalFileSize, numberOfFiles uint64
	var exhausted, ok bool

	for i := uint(0); i < n && !exhausted; i++ {
		// Initialise work pack
		workPack.Files = workPack.Files[:0]
		totalFileSize = 0
		numberOfFiles = 0

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

		err = p.enqueue(&workPack, workPackMap)
		if err != nil {
			return exhausted, err
		}
	}

	return exhausted, nil
}

// enqueue enqueues a WorkPack using the Producer's queueScheduler's Enqueue
// method.
// The second parameter, jobArgs, may be passed optionally to avoid
// reallocating a map on every call.
func (p *Producer) enqueue(workPack *workqueue.WorkPack, jobArgs map[string]interface{}) error {
	var err error

	if jobArgs == nil {
		jobArgs = make(map[string]interface{})
	}

	if len(workPack.Files) == 0 {
		return nil
	}

	err = workPack.ToJobArgs(jobArgs)
	if err != nil {
		return err
	}

	// Enqueue work pack
	_, err = p.queueScheduler.Enqueue(jobArgs)
	if err != nil {
		return err
	}

	return nil
}
