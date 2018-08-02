package workqueue

import (
	"bytes"
	"context"
	"time"

	"github.com/MasterOfBinary/gobatch/batch"
	"github.com/gocraft/work"
	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"gopkg.in/tomb.v2"

	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

//go:generate confions config WriteBackerConfig

type WriteBackerConfig struct {
	Batch batch.ConfigValues

	FileSystemName string
	Namespace      string

	RunId        uint64
	SnapshotName string

	Pool   *redis.Pool
	DB     *sqlx.DB
	Logger logrus.FieldLogger
}

var WriteBackerDefaultConfig = &WriteBackerConfig{
	Batch: batch.ConfigValues{
		MaxItems: 1000,
		MaxTime:  10 * time.Second,
	},
}

type WriteBacker struct {
	Config *WriteBackerConfig

	tomb *tomb.Tomb

	batch             *batch.Batch
	sourcePS          *batch.PipelineStage
	sourcePSAvailable chan struct{}

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
		w.batch.Go(
			w.tomb.Context(nil),
			writeBackerBatchSource{w},
			writeBackerBatchProcessor{w},
		)
		w.tomb.Go(w.batchDoneHandler)

		// Start the worker pool as soon as the "Source" pipeline stage is
		// available.
		w.tomb.Go(func() error {
			<-w.sourcePSAvailable

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
	w.tomb.Kill(stopSignalled)
}

func (w *WriteBacker) Wait() {
	<-w.tomb.Dead()
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
		w.fieldLogger.Debug("Stopping worker pool and closing source pipeline stage because component is dying")

		w.workerPool.Stop()
		w.sourcePS.Close()
	}

	return nil
}

func (w *WriteBacker) batchDoneHandler() error {
	select {
	case <-w.batch.Done():
		w.fieldLogger.WithField("action", "stopping").Info("Received batch-finished signal, stopping component")

		w.tomb.Kill(nil)
	case <-w.tomb.Dying():
		w.fieldLogger.Debug("Waiting for batch to finish after component is dying")

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
	calculatedChecksums := make(map[uint64][]byte)
	fileIds := make([]uint64, 0, 32)

	for item := range ps.Input {
		writeBackFile := item.Get().(WriteBackPackFile)

		calculatedChecksums[writeBackFile.Id] = writeBackFile.Checksum
		fileIds = append(fileIds, writeBackFile.Id)
	}

	tx, filesUpdatePrepStmt, warningsInsertPrepStmt, err := w.openTxAndStmts(ctx)
	if err != nil {
		// TODO Error
		ps.Errors <- err

		return
	}

	var file meda.File

	rows, err := meda.FilesQueryCtxFilesByIdsForShare(ctx, tx, fileIds)
	if err != nil {
		// TODO Error
		ps.Errors <- err

		_ = w.closeTxAndStmts(tx, filesUpdatePrepStmt, warningsInsertPrepStmt)
		return
	}

	for rows.Next() {
		// TODO
		// Check if context has been cancelled.

		err = rows.StructScan(&file)
		if err != nil {
			// TODO Error
			ps.Errors <- err

			w.fieldLogger.WithFields(logrus.Fields{
				"action": "skipping",
				"error":  err,
			}).Warn("Skipping database row because scanning into struct failed")

			continue
		}

		if !bytes.Equal(calculatedChecksums[file.Id], file.Checksum) {
			err = w.writeChecksumWarning(
				ctx,
				warningsInsertPrepStmt,
				&file,
				calculatedChecksums[file.Id],
			)
			if err != nil {
				// TODO Error
				ps.Errors <- err
			}
		}

		file.Checksum = calculatedChecksums[file.Id]
		file.LastRead = w.Config.RunId
		file.ToBeRead = 0

		// TODO result
		_, err = filesUpdatePrepStmt.ExecContext(ctx, &file)
		if err != nil {
			// TODO Error
			ps.Errors <- err
		}
	}

	if err = rows.Err(); err != nil {
		// TODO Error
		ps.Errors <- err
	}

	err = w.closeTxAndStmts(tx, filesUpdatePrepStmt, warningsInsertPrepStmt)
	if err != nil {
		// TODO Error
		ps.Errors <- err

		return
	}
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

	filesUpdatePrepStmt, err = meda.FilesPrepareUpdateChecksum(ctx, tx)
	if err != nil {
		// Close everything hitherto
		_ = tx.Commit()
		return
	}

	warningsInsertPrepStmt, err = meda.ChecksumWarningsPrepareInsert(ctx, tx)
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
		_ = tx.Commit()
		return
	}

	err = filesUpdatePrepStmt.Close()
	if err != nil {
		// Attempt cleanup
		_ = warningsInsertPrepStmt.Close()
		_ = tx.Commit()
		return
	}

	err = tx.Commit()
	if err != nil {
		return
	}

	return
}

func (w *writeBackerBatchProcessor) writeChecksumWarning(
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
		"file_last_read":    file.LastRead,
	}).Info("Discovered checksum mismatch, writing checksum warning")

	checksumWarning := meda.ChecksumWarning{
		FileId:           file.Id,
		Path:             file.Path,
		ModificationTime: file.ModificationTime,
		FileSize:         file.FileSize,
		ExpectedChecksum: file.Checksum,
		ActualChecksum:   checksum,
		Discovered:       w.Config.RunId,
		LastRead:         file.LastRead,
		Created:          time.Now(),
	}

	// TODO result
	_, err := warningsInsertPrepStmt.ExecContext(ctx, &checksumWarning)

	return err
}
