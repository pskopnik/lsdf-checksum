package workqueue

import (
	"context"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"gopkg.in/tomb.v2"

	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

//go:generate confions config ProducerConfig

type ProducerConfig struct {
	MinWorkPackFileSize uint64
	FetchRowBatchSize   uint64
	RowBufferSize       uint64

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
	FetchRowBatchSize:   1000,
	RowBufferSize:       1000,
}

type Producer struct {
	Config *ProducerConfig

	tomb *tomb.Tomb

	filesChan      chan meda.File
	lastRand       float64
	lastId         uint64
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

	p.lastRand = -1
	p.lastId = 0
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
	p.tomb.Kill(stopSignalled)
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

func (p *Producer) queueSchedulerWaiter() error {
	p.queueScheduler.Wait()

	return nil
}

func (p *Producer) rowFetcher() error {
	var err error
	var exhausted bool

	files := make([]meda.File, p.Config.FetchRowBatchSize)
	dying := p.tomb.Dying()

	defer close(p.filesChan)

	for !exhausted {
		files, err = p.fetchNextBatch(files)
		if err != nil {
			p.fieldLogger.WithError(err).WithFields(logrus.Fields{
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

// fetchNextBatch fetches the next batch of meda.File rows from the database.
// "Next" is enforced by the Producer's lastRand and lastId fields, the fields
// are also updated before returning.
//
// The passed in files are reset to the slice's capacity before fetching is
// started.
// The files slice is always returned in a valid state, i.e. files[:] only
// contains successfully fetched meda.File rows. This also holds when the
// returned error != nil.
//
// The end of rows can be detected by comparing len(files) and cap(files) for
// the returned meda.File slice. If the length is less than the capacity, the
// end of the table has been reached.
func (p *Producer) fetchNextBatch(files []meda.File) ([]meda.File, error) {
	// Reset files slice to full capacity
	files = files[:cap(files)]
	i := 0

	finalise := func() {
		files = files[:i]
		if i > 0 {
			p.lastRand = files[i-1].Rand
			p.lastId = files[i-1].Id
		}
	}

	rows, err := meda.FilesQueryCtxFilesToBeReadPaginated(
		p.tomb.Context(nil),
		p.Config.DB,
		p.lastRand,
		p.lastId,
		uint64(len(files)), // Limit number of returned rows to at most len(files)
	)
	if err != nil {
		finalise()
		return files, err
	}
	defer rows.Close()

	for ; rows.Next(); i++ {
		err = rows.StructScan(&files[i])
		if err != nil {
			finalise()
			return files, err
		}
	}
	if err := rows.Err(); err != nil {
		finalise()
		return files, err
	}

	finalise()
	return files, nil
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
	var totalFileSize uint64
	var exhausted, ok bool

	for i := uint(0); i < n && !exhausted; i++ {
		// Initialise work pack
		workPack.Files = workPack.Files[:0]
		totalFileSize = 0

		// Prepare work pack
		for totalFileSize < p.Config.MinWorkPackFileSize {
			// If the Producer is dying, filesChan is closed by rowFetcher().
			// Thus, this loop will also shut down quickly.
			file, ok = <-p.filesChan
			exhausted = !ok
			if exhausted {
				break
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
// The second parameter, jobArgs, may be passed optionally to avoid
// reallocating a map on every call.
func (p *Producer) enqueue(workPack *WorkPack, jobArgs map[string]interface{}) error {
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
