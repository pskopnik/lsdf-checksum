package workqueue

import (
	"bytes"
	"context"
	"time"

	"github.com/MasterOfBinary/gobatch/batch"
	"github.com/go-errors/errors"
	"github.com/gocraft/work"
	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"gopkg.in/tomb.v2"

	"git.scc.kit.edu/sdm/lsdf-checksum/internal/lifecycle"
	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

//go:generate confions config WriteBackerConfig

type WriteBackerConfig struct {
	Batch batch.ConfigValues

	FileSystemName string
	Namespace      string

	RunId        uint64
	SnapshotName string

	Pool   *redis.Pool        `yaml:"-"`
	DB     *meda.DB           `yaml:"-"`
	Logger logrus.FieldLogger `yaml:"-"`
}

var WriteBackerDefaultConfig = &WriteBackerConfig{
	// Process Batch if after MinTime MinItems have been received.
	// Process Batch earlier if MaxTime or MaxItems have been exceeded.
	Batch: batch.ConfigValues{
		MinTime:  1 * time.Second,
		MinItems: 1000,
		MaxTime:  10 * time.Second,
		MaxItems: 5000,
	},
}

type WriteBacker struct {
	Config *WriteBackerConfig

	tomb *tomb.Tomb

	batch             *batch.Batch
	sourcePS          *batch.PipelineStage
	sourcePSAvailable chan struct{}
	batchErrors       <-chan error

	workerPool       *work.WorkerPool
	endOfQueueSignal chan struct{}

	fieldLogger logrus.FieldLogger
}

func NewWriteBacker(config *WriteBackerConfig) *WriteBacker {
	return &WriteBacker{
		Config: config,
	}
}

func (w *WriteBacker) Start(ctx context.Context) {
	w.tomb, _ = tomb.WithContext(ctx)

	w.fieldLogger = w.Config.Logger.WithFields(logrus.Fields{
		"run":        w.Config.RunId,
		"snapshot":   w.Config.SnapshotName,
		"filesystem": w.Config.FileSystemName,
		"namespace":  w.Config.Namespace,
		"package":    "workqueue",
		"component":  "WriteBacker",
	})

	w.workerPool = work.NewWorkerPool(writeBackerContext{}, 1, w.Config.Namespace, w.Config.Pool)
	w.workerPool.Middleware(
		func(wBCtx *writeBackerContext, job *work.Job, next work.NextMiddlewareFunc) error {
			wBCtx.WriteBacker = w
			return next()
		},
	)
	jobName := WriteBackJobName(w.Config.FileSystemName, w.Config.SnapshotName)
	w.workerPool.Job(jobName, (*writeBackerContext).Process)

	w.sourcePSAvailable = make(chan struct{})
	w.batch = batch.New(writeBackerBatchConfig{w})

	w.endOfQueueSignal = make(chan struct{})

	w.tomb.Go(func() error {
		w.batchErrors = w.batch.Go(
			w.tomb.Context(nil),
			writeBackerBatchSource{w},
			writeBackerBatchProcessor{w},
		)
		w.tomb.Go(w.batchErrorsHandler)
		w.tomb.Go(w.batchManager)

		// Start the worker pool as soon as the "Source" pipeline stage is
		// available.
		w.tomb.Go(func() error {
			select {
			case <-w.sourcePSAvailable:
				break
			case <-w.tomb.Dying():
				return nil
			}

			w.workerPool.Start()
			w.tomb.Go(w.endOfQueueHandler)

			return nil
		})

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

		w.fieldLogger.Debug("Draining and stopping worker pool and closing source pipeline stage as end-of-queue is reached")
		w.workerPool.Drain()
		w.workerPool.Stop()
		w.sourcePS.Close()
	case <-w.tomb.Dying():
		w.fieldLogger.Debug("Stopping worker pool and closing source pipeline stage as component is dying")

		w.workerPool.Stop()
		w.sourcePS.Close()
	}

	return nil
}

func (w *WriteBacker) batchErrorsHandler() error {
	var err error

	for err = range w.batchErrors {
		entry := w.fieldLogger.WithError(err)

		if processorError, ok := err.(*batch.ProcessorError); ok {
			err = processorError.Original()
		}

		if errorsErr, ok := err.(*errors.Error); ok {
			entry = entry.WithField("stack_trace", errorsErr.ErrorStack())
		}

		entry.Warn("Encountered error during batch processing")
	}

	return nil
}

func (w *WriteBacker) batchManager() error {
	select {
	case <-w.batch.Done():
		w.fieldLogger.WithField("action", "stopping").Info("Received batch-finished signal, stopping component")

		w.tomb.Kill(nil)
	case <-w.tomb.Dying():
		w.fieldLogger.Debug("Waiting for batch to finish as component is dying")

		<-w.batch.Done()
	}

	return nil
}

type writeBackerContext struct {
	WriteBacker *WriteBacker
}

func (w *writeBackerContext) Process(job *work.Job) error {
	writeBackPack := WriteBackPack{}

	err := writeBackPack.FromJobArgs(job.Args)
	if err != nil {
		w.WriteBacker.fieldLogger.WithError(err).WithFields(logrus.Fields{
			"action":   "skipping",
			"args":     job.Args,
			"job_name": job.Name,
		}).Warn("Encountered error during WriteBackPack unmarshaling")

		return err
	}

	w.WriteBacker.fieldLogger.WithFields(logrus.Fields{
		"write_back_pack": writeBackPack,
	}).Debug("Received WriteBackPack")

	for _, file := range writeBackPack.Files {
		item := batch.NextItem(w.WriteBacker.sourcePS, file)
		w.WriteBacker.sourcePS.Output <- item
	}

	return nil
}

var _ batch.Config = writeBackerBatchConfig{
	WriteBacker: &WriteBacker{},
}

type writeBackerBatchConfig struct {
	*WriteBacker
}

func (w writeBackerBatchConfig) Get() batch.ConfigValues {
	return w.Config.Batch
}

var _ batch.Source = writeBackerBatchSource{
	WriteBacker: &WriteBacker{},
}

type writeBackerBatchSource struct {
	*WriteBacker
}

func (w writeBackerBatchSource) Read(_ context.Context, ps *batch.PipelineStage) {
	w.sourcePS = ps
	close(w.sourcePSAvailable)
}

var _ batch.Processor = writeBackerBatchProcessor{
	WriteBacker: &WriteBacker{},
}

type writeBackerBatchProcessor struct {
	*WriteBacker
}

func (w writeBackerBatchProcessor) Process(ctx context.Context, ps *batch.PipelineStage) {
	w.fieldLogger.Debug("Starting processing of batch")
	defer ps.Close()

	calculatedChecksums, fileIds := w.readBatchFiles(ps)

	tx, filesUpdatePrepStmt, warningsInsertPrepStmt, err := w.openTxAndStmts(ctx)
	if err != nil {
		// TODO Error
		ps.Errors <- errors.Wrap(err, 0)

		w.fieldLogger.WithError(err).Warn("a")

		return
	}

	files, err := w.fetchDBFiles(ctx, tx, fileIds)
	if err != nil {
		_ = w.closeTxAndStmts(tx, filesUpdatePrepStmt, warningsInsertPrepStmt)
		// TODO Error
		ps.Errors <- errors.Wrap(err, 0)

		w.fieldLogger.WithError(err).Warn("b")

		return
	}

	for ind, _ := range files {
		// Pointer to file in files, don't copy
		file := &files[ind]

		calculatedChecksum, ok := calculatedChecksums[file.Id]
		if !ok {
			w.fieldLogger.WithFields(logrus.Fields{
				"action":  "skipping",
				"file_id": file.Id,
			}).Warn("Database returned unknown file")
			continue
		} else if calculatedChecksum == nil {
			w.fieldLogger.WithFields(logrus.Fields{
				"action":  "skipping",
				"file_id": file.Id,
			}).Warn("Encountered duplicate file id")
			continue
		}

		if file.Checksum != nil && file.ToBeCompared == 1 &&
			!bytes.Equal(calculatedChecksum, file.Checksum) {
			err = w.issueChecksumWarning(
				ctx,
				warningsInsertPrepStmt,
				file,
				calculatedChecksum,
			)
			if err != nil {
				// TODO Error
				ps.Errors <- errors.Wrap(err, 0)
				w.fieldLogger.WithError(err).WithFields(logrus.Fields{
					"action":            "skipping",
					"file_id":           file.Id,
					"file_path":         file.Path,
					"file_size":         file.FileSize,
					"expected_checksum": file.Checksum,
					"actual_checksum":   calculatedChecksum,
					"file_last_read":    file.LastRead.Uint64,
				}).Warn("Encountered error while issuing checksum warning")
			}
		}

		file.Checksum = calculatedChecksums[file.Id]
		file.LastRead.Uint64, file.LastRead.Valid = w.Config.RunId, true
		file.ToBeRead = 0
		file.ToBeCompared = 0

		// TODO result
		_, err = filesUpdatePrepStmt.ExecContext(ctx, file)
		if err != nil {
			// TODO Error
			ps.Errors <- errors.Wrap(err, 0)

			w.fieldLogger.WithError(err).WithFields(logrus.Fields{
				"action":        "ignoring",
				"file_id":       file.Id,
				"file_path":     file.Path,
				"file_size":     file.FileSize,
				"file_checksum": file.Checksum,
			}).Warn("Encountered error while writing new checksum to database")
		}

		calculatedChecksums[file.Id] = nil
	}

	// Check that all files received from the batch's input channel
	// (calculatedChecksums) have been processed
	for id, checksum := range calculatedChecksums {
		if checksum != nil {
			w.fieldLogger.WithFields(logrus.Fields{
				"action":        "ignoring",
				"file_id":       id,
				"file_checksum": checksum,
			}).Warn("Database returned insufficient files")
		}
	}

	err = w.closeTxAndStmts(tx, filesUpdatePrepStmt, warningsInsertPrepStmt)
	if err != nil {
		// TODO Error
		ps.Errors <- errors.Wrap(err, 0)
		w.fieldLogger.WithError(err).Warn("f")

		return
	}

	w.fieldLogger.Debug("Finished processing of batch")
}

func (w writeBackerBatchProcessor) readBatchFiles(ps *batch.PipelineStage) (map[uint64][]byte, []uint64) {
	calculatedChecksums := make(map[uint64][]byte)
	fileIds := make([]uint64, 0, 32)

	for item := range ps.Input {
		writeBackFile := item.Get().(WriteBackPackFile)

		calculatedChecksums[writeBackFile.Id] = writeBackFile.Checksum
		fileIds = append(fileIds, writeBackFile.Id)
	}

	return calculatedChecksums, fileIds
}

func (w writeBackerBatchProcessor) fetchDBFiles(ctx context.Context, tx *sqlx.Tx, fileIds []uint64) ([]meda.File, error) {
	var file meda.File
	files := make([]meda.File, 0, len(fileIds))

	rows, err := w.Config.DB.FilesQueryFilesByIdsForShare(ctx, tx, fileIds)
	if err != nil {
		// TODO Error
		w.fieldLogger.WithError(err).Warn(".a")
		return nil, errors.Wrap(err, 0)
	}
	defer rows.Close()

	for rows.Next() {
		// TODO
		// Check if context has been cancelled.

		err = rows.StructScan(&file)
		if err != nil {
			// TODO Error
			w.fieldLogger.WithFields(logrus.Fields{
				"action": "skipping",
				"error":  err,
			}).Warn("Skipping database row because scanning into struct failed")

			continue
		}

		files = append(files, file)
	}

	if err = rows.Err(); err != nil {
		// TODO Error
		w.fieldLogger.WithError(err).Warn(".b")
		return nil, errors.Wrap(err, 0)
	}

	return files, nil
}

func (w writeBackerBatchProcessor) openTxAndStmts(ctx context.Context) (
	tx *sqlx.Tx,
	filesUpdatePrepStmt *sqlx.NamedStmt,
	warningsInsertPrepStmt *sqlx.NamedStmt,
	err error,
) {
	tx, err = w.Config.DB.BeginTxx(ctx, nil)
	if err != nil {
		return
	}

	filesUpdatePrepStmt, err = w.Config.DB.FilesPrepareUpdateChecksum(ctx, tx)
	if err != nil {
		// Close everything hitherto
		_ = tx.Commit()
		return
	}

	warningsInsertPrepStmt, err = w.Config.DB.ChecksumWarningsPrepareInsert(ctx, tx)
	if err != nil {
		// Close everything hitherto
		_ = filesUpdatePrepStmt.Close()
		_ = tx.Commit()
		return
	}

	return
}

func (w writeBackerBatchProcessor) closeTxAndStmts(
	tx *sqlx.Tx,
	filesUpdatePrepStmt *sqlx.NamedStmt,
	warningsInsertPrepStmt *sqlx.NamedStmt,
) (err error) {
	err = warningsInsertPrepStmt.Close()
	if err != nil {
		// Attempt cleanup
		_ = filesUpdatePrepStmt.Close()
		_ = tx.Commit()
		return
	}

	err = filesUpdatePrepStmt.Close()
	if err != nil {
		// Attempt cleanup
		_ = tx.Commit()
		return
	}

	err = tx.Commit()
	if err != nil {
		return
	}

	return
}

func (w *writeBackerBatchProcessor) issueChecksumWarning(
	ctx context.Context,
	warningsInsertPrepStmt *sqlx.NamedStmt,
	file *meda.File,
	checksum []byte,
) error {
	w.fieldLogger.WithFields(logrus.Fields{
		"file_id":           file.Id,
		"file_path":         file.Path,
		"file_size":         file.FileSize,
		"expected_checksum": file.Checksum,
		"actual_checksum":   checksum,
		"file_last_read":    file.LastRead.Uint64,
	}).Info("Discovered checksum mismatch, writing checksum warning")

	checksumWarning := meda.ChecksumWarning{
		FileId:           file.Id,
		Path:             file.Path,
		ModificationTime: file.ModificationTime,
		FileSize:         file.FileSize,
		ExpectedChecksum: file.Checksum,
		ActualChecksum:   checksum,
		Discovered:       w.Config.RunId,
		LastRead:         file.LastRead.Uint64,
		Created:          meda.Time(time.Now()),
	}

	_, err := warningsInsertPrepStmt.ExecContext(ctx, &checksumWarning)

	return err
}
