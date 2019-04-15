package worker

import (
	"context"
	"crypto/sha1"
	"hash"
	"io"
	"path/filepath"
	"time"

	"golang.org/x/time/rate"

	"github.com/apex/log"
	"github.com/gocraft/work"
	"github.com/gomodule/redigo/redis"
	"gopkg.in/tomb.v2"

	"git.scc.kit.edu/sdm/lsdf-checksum/internal/lifecycle"
	"git.scc.kit.edu/sdm/lsdf-checksum/lengthsafe"
	"git.scc.kit.edu/sdm/lsdf-checksum/master/workqueue"
	"git.scc.kit.edu/sdm/lsdf-checksum/ratedreader"
	commonRedis "git.scc.kit.edu/sdm/lsdf-checksum/redis"
)

const bufferSize int = 32 * 1024

type Config struct {
	Concurrency   int
	MaxThroughput int

	Logger log.Interface `yaml:"-"`

	Redis       commonRedis.Config
	RedisPrefix string
}

var DefaultConfig = &Config{
	Concurrency:   1,
	MaxThroughput: 10 * 1024 * 1024,
}

type Worker struct {
	Config *Config

	tomb *tomb.Tomb

	pool       *redis.Pool
	workerPool *work.WorkerPool
	enqueuer   *work.Enqueuer

	prefixer *Prefixer

	// Pools

	ratedReaders chan *ratedreader.Reader
	hashers      chan hash.Hash
	buffers      chan []byte

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

	w.tomb, _ = tomb.WithContext(ctx)

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
	w.enqueuer = work.NewEnqueuer(gocraftWorkNamespace, w.pool)

	w.workerPool = work.NewWorkerPool(workerContext{}, uint(w.Config.Concurrency), gocraftWorkNamespace, w.pool)
	w.workerPool.Middleware(
		func(workerCtx *workerContext, job *work.Job, next work.NextMiddlewareFunc) error {
			workerCtx.Worker = w
			return next()
		},
	)
	jobName := workqueue.CalculateChecksumJobName
	w.workerPool.Job(jobName, (*workerContext).CalculateChecksum)

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

func (w *Worker) createPrefixer() *Prefixer {
	return NewPrefixer(&PrefixerConfig{
		TTL:             30 * time.Minute,
		ReapingInterval: time.Hour,
		Logger:          w.Config.Logger,
	})
}

func (w *Worker) initPools() {
	w.ratedReaders = make(chan *ratedreader.Reader, w.Config.Concurrency)
	for i := 0; i < w.Config.Concurrency; i++ {
		reader := ratedreader.NewReader(nil, rate.Limit(float64(w.Config.MaxThroughput)/float64(w.Config.Concurrency)))
		w.ratedReaders <- reader
	}

	w.hashers = make(chan hash.Hash, w.Config.Concurrency)
	for i := 0; i < w.Config.Concurrency; i++ {
		hasher := sha1.New()
		w.hashers <- hasher
	}

	w.buffers = make(chan []byte, w.Config.Concurrency)
	for i := 0; i < w.Config.Concurrency; i++ {
		buffer := make([]byte, bufferSize)
		w.buffers <- buffer
	}
}

type workerContext struct {
	Worker *Worker

	hasher hash.Hash
	reader *ratedreader.Reader
	buffer []byte
}

func (w *workerContext) CalculateChecksum(job *work.Job) error {
	workPack := workqueue.WorkPack{}

	w.Worker.fieldLogger.WithFields(log.Fields{
		"args":     job.Args,
		"job_name": job.Name,
	}).Debug("Received work")

	err := workPack.FromJobArgs(job.Args)
	if err != nil {
		w.Worker.fieldLogger.WithError(err).WithFields(log.Fields{
			"action":   "skipping",
			"args":     job.Args,
			"job_name": job.Name,
		}).Warn("Encountered error during WorkPack unmarshaling")

		return err
	}

	prefix, err := w.Worker.prefixer.Prefix(&workPack)
	if err != nil {
		w.Worker.fieldLogger.WithError(err).WithFields(log.Fields{
			"action":     "skipping",
			"filesystem": workPack.FileSystemName,
			"snapshot":   workPack.SnapshotName,
			"job_name":   job.Name,
		}).Warn("Encountered error while determining path prefix")

		return err
	}

	writeBackPack := workqueue.WriteBackPack{}
	writeBackPack.Files = make([]workqueue.WriteBackPackFile, 0, len(workPack.Files))

	w.getFromPools()

	for _, file := range workPack.Files {
		path := filepath.Join(prefix, file.Path)
		fileReader, err := lengthsafe.Open(path)
		if err != nil {
			w.Worker.fieldLogger.WithError(err).WithFields(log.Fields{
				"action":     "skipping",
				"filesystem": workPack.FileSystemName,
				"snapshot":   workPack.SnapshotName,
				"job_name":   job.Name,
				"id":         file.ID,
				"path":       path,
			}).Warn("Encountered error while opening file")
			continue
		}

		w.reader.ResetReader(fileReader)
		w.hasher.Reset()

		n, err := io.CopyBuffer(w.hasher, w.reader, w.buffer)
		if err != nil {
			_ = fileReader.Close()
			w.Worker.fieldLogger.WithError(err).WithFields(log.Fields{
				"action":     "skipping",
				"filesystem": workPack.FileSystemName,
				"snapshot":   workPack.SnapshotName,
				"job_name":   job.Name,
				"id":         file.ID,
				"path":       path,
			}).Warn("Encountered error while calculating hashsum of file")
			continue
		}

		err = fileReader.Close()
		if err != nil {
			w.Worker.fieldLogger.WithError(err).WithFields(log.Fields{
				"action":     "skipping",
				"filesystem": workPack.FileSystemName,
				"snapshot":   workPack.SnapshotName,
				"job_name":   job.Name,
				"id":         file.ID,
				"path":       path,
			}).Warn("Encountered error while closing file")
			continue
		}

		checksum := w.hasher.Sum(nil)

		w.Worker.fieldLogger.WithFields(log.Fields{
			"filesystem": workPack.FileSystemName,
			"snapshot":   workPack.SnapshotName,
			"job_name":   job.Name,
			"id":         file.ID,
			"path":       path,
			"bytes_read": n,
			"checksum":   checksum,
		}).Debug("Read file")

		writeBackPack.Files = append(writeBackPack.Files, workqueue.WriteBackPackFile{
			ID:       file.ID,
			Checksum: checksum,
		})
	}

	w.returnToPools()

	err = w.enqueueWriteBackPack(&workPack, &writeBackPack)
	if err != nil {
		w.Worker.fieldLogger.WithError(err).WithFields(log.Fields{
			"action":          "skipping",
			"filesystem":      workPack.FileSystemName,
			"snapshot":        workPack.SnapshotName,
			"job_name":        job.Name,
			"write_back_pack": &writeBackPack,
		}).Warn("Encountered error while enqueueing WriteBackPack")
		return err
	}

	return nil
}

func (w *workerContext) getFromPools() {
	w.reader = <-w.Worker.ratedReaders
	w.hasher = <-w.Worker.hashers
	w.buffer = <-w.Worker.buffers
}

func (w *workerContext) returnToPools() {
	w.reader.ResetReader(nil)
	w.Worker.ratedReaders <- w.reader

	w.hasher.Reset()
	w.Worker.hashers <- w.hasher

	w.Worker.buffers <- w.buffer
}

func (w *workerContext) enqueueWriteBackPack(workPack *workqueue.WorkPack, writeBackPack *workqueue.WriteBackPack) error {
	var err error

	jobName := workqueue.WriteBackJobName(workPack.FileSystemName, workPack.SnapshotName)

	jobArgs := make(map[string]interface{})

	err = writeBackPack.ToJobArgs(jobArgs)
	if err != nil {
		return err
	}

	_, err = w.Worker.enqueuer.Enqueue(jobName, jobArgs)
	if err != nil {
		return err
	}

	return nil
}
