package workqueue

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/apex/log"
	"github.com/gocraft/work"
	"github.com/gomodule/redigo/redis"
	pkgErrors "github.com/pkg/errors"
	"gopkg.in/tomb.v2"

	"git.scc.kit.edu/sdm/lsdf-checksum/internal/lifecycle"
	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

var (
	ErrFetchedInsufficientFiles = errors.New("database returned insufficient files, some queried files are missing")
	ErrFetchedUnexpectedFile    = errors.New("database returned unexpected file, id not included in query")
	ErrFetchedDuplicateFile     = errors.New("database returned duplicate file")
)

//go:generate confions config WriteBackerConfig

type WriteBackerConfig struct {
	Batcher       batcherConfig
	Transactioner transactionerConfig

	FileSystemName string
	Namespace      string

	RunID        uint64
	SnapshotName string

	Pool   *redis.Pool   `yaml:"-"`
	DB     *meda.DB      `yaml:"-"`
	Logger log.Interface `yaml:"-"`
}

var WriteBackerDefaultConfig = &WriteBackerConfig{
	Batcher: batcherConfig{
		// Complete batch after 500 items (as identified in checksum_write_back
		// benchmark).
		MaxItems: 500,
		// Complete batch 30 s after first item has been added.
		MaxWaitTime: 30 * time.Second,
		// Only allow 1 batch to be in-flight.
		BatchBufferSize: 1,
	},
	Transactioner: transactionerConfig{
		// Commit transaction once 20 write queries have been performed (as
		// identified in checksum_write_back benchmark).
		MaxTransactionSize: 20,
		// Commit transaction after 2 minutes regardless of the number of
		// queries performed.
		MaxTransactionLifetime: 2 * time.Minute,
	},
}

type WriteBacker struct {
	Config *WriteBackerConfig

	tomb *tomb.Tomb

	batcher *batcher

	endOfQueueSignal  chan struct{}
	workerPool        *work.WorkerPool
	workerPoolStopped chan struct{}

	fieldLogger log.Interface
}

func NewWriteBacker(config *WriteBackerConfig) *WriteBacker {
	return &WriteBacker{
		Config: config,
	}
}

func (w *WriteBacker) Start(ctx context.Context) {
	w.tomb, _ = tomb.WithContext(ctx)

	w.fieldLogger = w.Config.Logger.WithFields(log.Fields{
		"run":        w.Config.RunID,
		"snapshot":   w.Config.SnapshotName,
		"filesystem": w.Config.FileSystemName,
		"namespace":  w.Config.Namespace,
		"component":  "workqueue.WriteBacker",
	})

	w.endOfQueueSignal = make(chan struct{})
	w.workerPoolStopped = make(chan struct{})

	w.batcher = w.createBatcher()
	w.workerPool = w.createWorkerPool()

	w.tomb.Go(func() error {
		concurrency, err := w.Config.DB.ServerConcurrency()
		if err != nil {
			return pkgErrors.Wrap(err, "(*WriteBacker).Start")
		}

		w.batcher.Start(w.tomb.Context(nil))

		for i := 0; i < concurrency; i++ {
			w.tomb.Go(w.processor)
		}

		w.workerPool.Start()

		w.tomb.Go(w.endOfQueueHandler)

		w.tomb.Go(w.batcherManager)
		w.tomb.Go(w.workerPoolManager)

		return nil
	})
}

func (w *WriteBacker) SignalEndOfQueue() {
	close(w.endOfQueueSignal)
}

func (w *WriteBacker) SignalStop() {
	w.tomb.Kill(lifecycle.ErrStopSignalled)
}

func (w *WriteBacker) Wait() error {
	return w.tomb.Wait()
}

func (w *WriteBacker) Dead() <-chan struct{} {
	return w.tomb.Dead()
}

func (w *WriteBacker) Err() error {
	return w.tomb.Err()
}

func (w *WriteBacker) endOfQueueHandler() error {
	select {
	case <-w.endOfQueueSignal:
		w.fieldLogger.Info("Received end-of-queue signal")

		w.fieldLogger.Debug("Draining and stopping worker pool as end-of-queue is reached")
		w.workerPool.Drain()
		w.workerPool.Stop()
		close(w.workerPoolStopped)

		w.fieldLogger.Debug("Closing batcher as end-of-queue is reached")
		err := w.batcher.Close(w.tomb.Context(nil))
		if err != nil {
			err = pkgErrors.Wrap(err, "(*WriteBacker).endOfQueueHandler")
			w.fieldLogger.WithError(err).WithFields(log.Fields{
				"action": "stopping",
			}).Error("Encountered error while closing batcher")
			return err
		}
		return nil
	case <-w.tomb.Dying():
		return tomb.ErrDying
	}
}

func (w *WriteBacker) workerPoolManager() error {
	select {
	case <-w.workerPoolStopped:
		// There is no way of receiving errors from the worker pool
		return nil
	case <-w.tomb.Dying():
		w.fieldLogger.Debug("Stopping and waiting for worker pool as component is dying")
		w.workerPool.Stop()
		return tomb.ErrDying
	}
}

func (w *WriteBacker) batcherManager() error {
	select {
	case <-w.batcher.Dead():
		err := w.batcher.Err()
		if err == lifecycle.ErrStopSignalled {
			return nil
		} else if err != nil {
			err = pkgErrors.Wrap(err, "(*WriteBacker).batcherManager")
			w.fieldLogger.WithError(err).WithFields(log.Fields{
				"action": "stopping",
			}).Error("Batcher died")
			return err
		}

		return nil
	case <-w.tomb.Dying():
		w.fieldLogger.Debug("Waiting for batcher to finish as component is dying")
		_ = w.batcher.Wait()
		return tomb.ErrDying
	}
}

func (w *WriteBacker) createBatcher() *batcher {
	config := &batcherConfig{}
	*config = w.Config.Batcher

	return newBatcher(config)
}

func (w *WriteBacker) createWorkerPool() *work.WorkerPool {
	workerPool := work.NewWorkerPool(writeBackerContext{}, 1, w.Config.Namespace, w.Config.Pool)
	workerPool.Middleware(
		func(wBCtx *writeBackerContext, job *work.Job, next work.NextMiddlewareFunc) error {
			wBCtx.WriteBacker = w
			return next()
		},
	)
	jobName := WriteBackJobName(w.Config.FileSystemName, w.Config.SnapshotName)
	workerPool.Job(jobName, (*writeBackerContext).Process)

	return workerPool
}

func (w *WriteBacker) createTransactioner() *transactioner {
	config := &transactionerConfig{}
	*config = w.Config.Transactioner
	config.DB = w.Config.DB

	return newTransactioner(w.tomb.Context(nil), config)
}

func (w *WriteBacker) processor() error {
	ctx := w.tomb.Context(nil)
	transactioner := w.createTransactioner()

	batchChan := w.batcher.Out()
	dying := w.tomb.Dying()

	for {
		select {
		case batch, ok := <-batchChan:
			if !ok {
				transactioner.Commit()
				return nil
			}

			err := w.processBatch(ctx, batch, transactioner)
			batch.Return()
			if err != nil {
				err = pkgErrors.Wrap(err, "(*WriteBacker).processor")
				w.fieldLogger.WithError(err).WithFields(log.Fields{
					"action": "stopping",
				}).Error("Encountered error while processing batch")
				transactioner.Close()
				return err
			}
		case <-dying:
			transactioner.Close()
			return tomb.ErrDying
		}
	}
}

func (w *WriteBacker) processBatch(ctx context.Context, batch *filesBatch, transactioner *transactioner) error {
	w.fieldLogger.Debug("Starting processing of batch")

	checksums, fileIDs := w.collectFilesInBatch(batch)

	// TODO pool files
	var files []meda.File

	files, err := transactioner.AppendFilesByIDs(files, ctx, fileIDs)
	if err != nil {
		return pkgErrors.Wrap(err, "(*WriteBacker).processBatch: fetch files from database")
	}

	for i := range files {
		// Pointer to file in files, don't copy
		file := &files[i]

		checksum, ok := checksums[file.ID]
		if !ok {
			return pkgErrors.Wrapf(ErrFetchedUnexpectedFile, "(*WriteBacker).processBatch: process file with id %d", file.ID)
		} else if checksum == nil {
			return pkgErrors.Wrapf(ErrFetchedDuplicateFile, "(*WriteBacker).processBatch: process file with id %d", file.ID)
		}
		checksums[file.ID] = nil

		if file.Checksum != nil && file.ToBeCompared == 1 && !bytes.Equal(checksum, file.Checksum) {
			// info logging is handled by issueChecksumWarning
			err = w.issueChecksumWarning(ctx, file, checksum, transactioner)
			if err != nil {
				// error logging is handled by issueChecksumWarning (escalating)
				return pkgErrors.Wrapf(err, "(*WriteBacker).processBatch: process file with id %d", file.ID)
			}
		}

		file.Checksum = checksum
		file.LastRead.Uint64, file.LastRead.Valid = w.Config.RunID, true
		file.ToBeRead = 0
		file.ToBeCompared = 0
	}

	// Check that all files in the batch have been processed
	for _, checksum := range checksums {
		if checksum != nil {
			return pkgErrors.Wrap(ErrFetchedInsufficientFiles, "(*WriteBacker).processBatch: check all files processed")
		}
	}

	err = transactioner.UpdateFilesChecksums(ctx, files, w.Config.RunID)
	if err != nil {
		return pkgErrors.Wrap(err, "(*WriteBacker).processBatch: update files in database")
	}

	w.fieldLogger.Debug("Finished processing of batch")

	return nil
}

func (w *WriteBacker) collectFilesInBatch(batch *filesBatch) (map[uint64][]byte, []uint64) {
	checksums := make(map[uint64][]byte)
	// TODO use pool for fileIDs
	fileIDs := make([]uint64, 0, len(batch.Files))

	for i := range batch.Files {
		file := &batch.Files[i]

		if checksum, ok := checksums[file.ID]; ok {
			w.fieldLogger.WithFields(log.Fields{
				"action":              "skipping",
				"file_id":             file.ID,
				"first_checksum":      checksum,
				"subsequent_checksum": file.Checksum,
			}).Warn("Received same file multiple times in job batch, dropping all but first encounter")
			continue
		}

		checksums[file.ID] = file.Checksum
		fileIDs = append(fileIDs, file.ID)
	}

	return checksums, fileIDs
}

func (w *WriteBacker) issueChecksumWarning(ctx context.Context, file *meda.File, checksum []byte, transactioner *transactioner) error {
	w.fieldLogger.WithFields(log.Fields{
		"file_id":                file.ID,
		"file_path":              file.Path,
		"file_modification_time": file.ModificationTime,
		"file_size":              file.FileSize,
		"expected_checksum":      file.Checksum,
		"actual_checksum":        checksum,
		"file_last_read":         file.LastRead.Uint64,
	}).Info("Discovered checksum mismatch, writing checksum warning")

	checksumWarning := meda.ChecksumWarning{
		FileID:           file.ID,
		Path:             file.Path,
		ModificationTime: file.ModificationTime,
		FileSize:         file.FileSize,
		ExpectedChecksum: file.Checksum,
		ActualChecksum:   checksum,
		Discovered:       w.Config.RunID,
		LastRead:         file.LastRead.Uint64,
		Created:          meda.Time(time.Now()),
	}

	err := transactioner.InsertChecksumWarning(ctx, &checksumWarning)
	if err != nil {
		err = pkgErrors.Wrap(err, "(*WriteBacker).issueChecksumWarning")
		w.fieldLogger.WithError(err).WithFields(log.Fields{
			"action":                 "escalating",
			"file_id":                file.ID,
			"file_path":              file.Path,
			"file_modification_time": file.ModificationTime,
			"file_size":              file.FileSize,
			"expected_checksum":      file.Checksum,
			"actual_checksum":        checksum,
			"file_last_read":         file.LastRead.Uint64,
		}).Error("Encountered error while issuing checksum warning")
	}

	return nil
}

type writeBackerContext struct {
	WriteBacker *WriteBacker
}

func (w *writeBackerContext) Process(job *work.Job) error {
	ctx := w.WriteBacker.tomb.Context(nil)
	// TODO pool
	writeBackPack := WriteBackPack{}

	err := writeBackPack.FromJobArgs(job.Args)
	if err != nil {
		err = pkgErrors.Wrap(err, "(*writeBackerContext).Process: unmarshal WriteBackPack from job")
		w.WriteBacker.fieldLogger.WithError(err).WithFields(log.Fields{
			"action":   "stopping",
			"args":     job.Args,
			"job_name": job.Name,
		}).Error("Encountered error while unmarshaling WriteBackPack from job")
		w.WriteBacker.tomb.Kill(err)

		// return nil as to not re-queue the job
		return nil
	}

	w.WriteBacker.fieldLogger.WithFields(log.Fields{
		"write_back_pack": writeBackPack,
	}).Debug("Received and unmarshaled WriteBackPack")

	for i := range writeBackPack.Files {
		// Pointer to file in files, don't copy
		file := &writeBackPack.Files[i]

		err := w.WriteBacker.batcher.Add(ctx, file)
		if err != nil {
			err = pkgErrors.Wrap(err, "(*writeBackerContext).Process: add received file to batcher")
			w.WriteBacker.fieldLogger.WithError(err).WithFields(log.Fields{
				"action":        "stopping",
				"file_id":       file.ID,
				"file_checksum": file.Checksum,
			}).Error("Encountered error while adding received file to batcher")
			w.WriteBacker.tomb.Kill(err)

			// return nil as to not re-queue the job
			return nil
		}
	}

	return nil
}
