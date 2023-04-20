package worker

import (
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"golang.org/x/time/rate"

	"github.com/apex/log"
	"github.com/gocraft/work"
	"github.com/gomodule/redigo/redis"
	"gopkg.in/tomb.v2"

	"git.scc.kit.edu/sdm/lsdf-checksum/internal/lengthsafe"
	"git.scc.kit.edu/sdm/lsdf-checksum/internal/lifecycle"
	"git.scc.kit.edu/sdm/lsdf-checksum/internal/ratedreader"
	commonRedis "git.scc.kit.edu/sdm/lsdf-checksum/redis"
	"git.scc.kit.edu/sdm/lsdf-checksum/workqueue"
)

type Config struct {
	Concurrency   int
	MaxThroughput int
	FileReadSize  int

	Logger log.Interface `yaml:"-"`

	Redis       commonRedis.Config
	RedisPrefix string

	Workqueue workqueue.Config

	PrefixTTL                time.Duration
	PrefixerReapingInterval  time.Duration
	WorkqueueReapingInterval time.Duration
}

var DefaultConfig = &Config{
	Concurrency:              1,
	MaxThroughput:            10 * 1024 * 1024,
	FileReadSize:             32 * 1024,
	PrefixTTL:                30 * time.Minute,
	PrefixerReapingInterval:  time.Hour,
	WorkqueueReapingInterval: time.Hour,
}

type Worker struct {
	Config *Config

	ctx  context.Context
	tomb *tomb.Tomb

	pool       *redis.Pool
	workerPool *work.WorkerPool

	workqueues WorkqueuesKeeper
	prefixer   *Prefixer

	localLimiter *rate.Limiter

	// Pools

	buffers chan []byte

	fieldLogger log.Interface
}

func New(config *Config) *Worker {
	return &Worker{
		Config: config,
	}
}

func (w *Worker) Start(ctx context.Context) {
	w.fieldLogger = w.Config.Logger.WithFields(log.Fields{
		"component": "worker.Worker",
	})

	limit := rate.Inf
	if w.Config.MaxThroughput > 0 {
		limit = rate.Limit(w.Config.MaxThroughput)
	}
	w.localLimiter = rate.NewLimiter(limit, w.Config.FileReadSize)

	w.tomb, w.ctx = tomb.WithContext(ctx)

	w.workqueues = *w.createWorkqueuesKeeper()
	w.workqueues.Start()

	w.prefixer = w.createPrefixer()
	w.prefixer.Start(w.tomb.Context(nil))

	w.initPools()

	w.tomb.Go(func() error {
		pool, err := w.createRedisPool()
		if err != nil {
			return err
		}

		w.pool = pool

		w.tomb.Go(w.runWorkerPool)

		return nil
	})
}

func (w *Worker) SignalStop() {
	w.tomb.Kill(lifecycle.ErrStopSignalled)
}

func (w *Worker) Wait() error {
	return w.tomb.Wait()
}

func (w *Worker) Dead() <-chan struct{} {
	return w.tomb.Dead()
}

func (w *Worker) Err() error {
	return w.tomb.Err()
}

func (w *Worker) runWorkerPool() error {
	gocraftWorkNamespace := workqueue.GocraftWorkNamespace(w.Config.RedisPrefix)

	w.workerPool = work.NewWorkerPool(workerContext{}, uint(w.Config.Concurrency), gocraftWorkNamespace, w.pool)
	w.workerPool.Middleware(
		func(workerCtx *workerContext, _ *work.Job, next work.NextMiddlewareFunc) error {
			workerCtx.Worker = w
			return next()
		},
	)

	w.workerPool.Job(workqueue.ComputeChecksumJobName, (*workerContext).ComputeChecksum)

	w.workerPool.Start()

	<-w.tomb.Dying()

	w.workerPool.Stop()

	return nil
}

func (w *Worker) createRedisPool() (*redis.Pool, error) {
	config := commonRedis.DefaultConfig.
		Clone().
		Merge(&w.Config.Redis).
		Merge(&commonRedis.Config{})

	pool, err := commonRedis.CreatePool(config)
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func (w *Worker) createWorkqueuesKeeper() *WorkqueuesKeeper {
	return NewWorkqueuesKeeper(WorkqueuesKeeperConfig{
		Context:         w.ctx,
		Pool:            w.pool,
		Prefix:          w.Config.RedisPrefix,
		FileReadSize:    w.Config.FileReadSize,
		Logger:          w.Config.Logger,
		Workqueue:       w.Config.Workqueue,
		ReapingInterval: w.Config.WorkqueueReapingInterval,
	})
}

func (w *Worker) createPrefixer() *Prefixer {
	return NewPrefixer(&PrefixerConfig{
		TTL:             w.Config.PrefixTTL,
		ReapingInterval: w.Config.PrefixerReapingInterval,
		Logger:          w.Config.Logger,
	})
}

func (w *Worker) initPools() {
	w.buffers = make(chan []byte, w.Config.Concurrency)
	for i := 0; i < w.Config.Concurrency; i++ {
		buffer := make([]byte, w.Config.FileReadSize)
		w.buffers <- buffer
	}
}

type workerContext struct {
	Worker *Worker

	buffer []byte
}

func (w *workerContext) ComputeChecksum(job *work.Job) error {
	var workPack workqueue.WorkPack

	fieldLogger := w.Worker.fieldLogger.WithFields(log.Fields{
		"job_name": job.Name,
		"job_id":   job.ID,
	})

	err := workPack.FromJobArgs(job.Args)
	if err != nil {
		fieldLogger.WithError(err).WithFields(log.Fields{
			"action": "failing-job",
			"args":   job.Args,
		}).Warn("Encountered error during WorkPack unmarshaling")

		return err
	}

	fieldLogger = fieldLogger.WithFields(log.Fields{
		"filesystem": workPack.FileSystemName,
		"snapshot":   workPack.SnapshotName,
	})
	fieldLogger.Debug("Starting processing job")

	wqCtx, err := w.Worker.workqueues.Get(workPack.FileSystemName, workPack.SnapshotName)
	if err != nil {
		fieldLogger.WithError(err).WithFields(log.Fields{
			"action": "failing-job",
		}).Warn("Encountered error while getting workqueue instance")

		return err
	}

	prefix, err := w.Worker.prefixer.Prefix(&workPack)
	if err != nil {
		fieldLogger.WithError(err).WithFields(log.Fields{
			"action": "failing-job",
		}).Warn("Encountered error while determining path prefix")

		return err
	}

	var writeBackPack workqueue.WriteBackPack
	writeBackPack.Files = make([]workqueue.WriteBackPackFile, 0, len(workPack.Files))

	// gets expensive objects from pool and stores in w
	w.getFromPools()

	limiters := [...]*rate.Limiter{
		w.Worker.localLimiter,
		wqCtx.Limiter,
	}

	for _, file := range workPack.Files {
		path := filepath.Join(prefix, file.Path)
		fieldLogger := fieldLogger.WithFields(log.Fields{
			"id":   file.ID,
			"path": path,
		})

		n, checksum, err := w.readFileAndComputeChecksum(path, limiters[:])
		if err != nil {
			fieldLogger.WithError(err).WithFields(log.Fields{
				"action": "failing-job",
			}).Warn("Encountered error while computing checksum of file")
			continue
		}

		fieldLogger.WithFields(log.Fields{
			"bytes_read": n,
			"checksum":   checksum,
		}).Debug("Read file and computed checksum")

		writeBackPack.Files = append(writeBackPack.Files, workqueue.WriteBackPackFile{
			ID:       file.ID,
			Checksum: checksum,
		})
	}

	w.returnToPools()

	_, err = wqCtx.WriteBackEnqueuer.Enqueue(&writeBackPack)
	if err != nil {
		fieldLogger.WithError(err).WithFields(log.Fields{
			"action":          "failing-job",
			"write_back_pack": &writeBackPack,
		}).Warn("Encountered error while enqueueing WriteBackPack")
		return err
	}

	return nil
}

func (w *workerContext) readFileAndComputeChecksum(
	path string, limiters []*rate.Limiter,
) (int64, []byte, error) {
	fileReader, err := lengthsafe.Open(path)
	if err != nil {
		return 0, nil, fmt.Errorf("opening file: %w", err)
	}
	defer fileReader.Close()

	reader := ratedreader.NewMultiReader(fileReader, limiters[:])
	hasher := sha1.New()

	n, err := io.CopyBuffer(hasher, reader, w.buffer)
	if err != nil {
		return 0, nil, fmt.Errorf("reading file and computing checksum: %w", err)
	}

	err = fileReader.Close()
	if err != nil {
		return 0, nil, fmt.Errorf("closing file: %w", err)
	}

	checksum := hasher.Sum(nil)

	return n, checksum, nil
}

func (w *workerContext) getFromPools() {
	w.buffer = <-w.Worker.buffers
}

func (w *workerContext) returnToPools() {
	w.Worker.buffers <- w.buffer
}
