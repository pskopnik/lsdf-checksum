// Package medasync contains the Syncer component for synchronising file
// system data with the meta data database.
package medasync

import (
	"context"
	"database/sql"
	"io"
	"path/filepath"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/apex/log"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"

	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt"
	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt/filelist"
	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt/options"
)

var (
	txOptionsReadCommitted = &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	}
)

func beginTxWithReadCommitted(ctx context.Context, db *meda.DB) (*sqlx.Tx, error) {
	tx, err := db.BeginTxx(ctx, txOptionsReadCommitted)
	if err != nil {
		return nil, errors.Wrap(err, "beginTxWithReadCommitted")
	}

	return tx, nil
}

//go:generate confions config Config

type Config struct {
	// Static config

	// MaxTransactionSize is the maximum number of commands per SQL
	// transaction.
	// After this number of commands, the transaction will be committed
	// and a new one will be begun.
	MaxTransactionSize       int
	SynchronisationChunkSize uint64

	// Static Params

	Subpath             string
	TemporaryDirectory  string
	GlobalWorkDirectory string
	NodeList            []string
	Location            *time.Location `yaml:"-"`

	// Invocation dependent params

	SnapshotName string
	RunID        uint64
	SyncMode     meda.RunSyncMode

	// Dynamic objects

	DB         *meda.DB              `yaml:"-"`
	FileSystem *scaleadpt.FileSystem `yaml:"-"`
	Logger     log.Interface         `yaml:"-"`
}

var DefaultConfig = Config{
	MaxTransactionSize:       10000,
	SynchronisationChunkSize: 100000,
	Location:                 time.UTC,
}

type Syncer struct {
	Config *Config

	fieldLogger log.Interface
	basePath    string
}

func New(config *Config) *Syncer {
	return &Syncer{
		Config: config,
	}
}

func (s *Syncer) Run(ctx context.Context) error {
	var err error

	s.fieldLogger = s.createFieldLogger()

	// Perform the first step of the Syncer in parallel

	var parser *filelist.CloseParser
	var errg errgroup.Group

	errg.Go(func() (err error) {
		parser, err = s.applyPolicy()
		return
	})

	errg.Go(func() (err error) {
		err = s.prepareDatabase(ctx)
		return
	})

	err = errg.Wait()
	if err != nil {
		if parser != nil {
			_ = parser.Close()
		}

		return err
	}

	parser.Loc = s.Config.Location

	err = s.writeInserts(ctx, &parser.Parser)
	if err != nil {
		_ = parser.Close()
		return err
	}

	err = parser.Close()
	if err != nil {
		return err
	}

	err = s.syncDatabase(ctx)
	if err != nil {
		return err
	}

	err = s.cleanUpDatabase(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (s *Syncer) CleanUp(ctx context.Context) error {
	s.fieldLogger = s.createFieldLogger()

	return s.cleanUpDatabase(ctx)
}

func (s *Syncer) createFieldLogger() log.Interface {
	return s.Config.Logger.WithFields(log.Fields{
		"sync_mode":  s.Config.SyncMode,
		"filesystem": s.Config.FileSystem.GetName(),
		"run":        s.Config.RunID,
		"snapshot":   s.Config.SnapshotName,
		"subpath":    s.Config.Subpath,
		"component":  "medasync.Syncer",
	})
}

func (s *Syncer) prepareDatabase(ctx context.Context) error {
	s.fieldLogger.Info("Starting preparing the meta data database")

	err := s.truncateInserts(ctx)
	if err != nil {
		return errors.Wrap(err, "(*Syncer).prepareDatabase")
	}

	s.fieldLogger.Info("Finished preparing the meta data database")

	return nil
}

func (s *Syncer) applyPolicy() (*filelist.CloseParser, error) {
	options := []options.PolicyOptioner{
		scaleadpt.PolicyOpt.SnapshotName(s.Config.SnapshotName),
		scaleadpt.PolicyOpt.Subpath(s.Config.Subpath),
		scaleadpt.PolicyOpt.TempDir(s.Config.TemporaryDirectory),
	}

	fields := log.Fields{
		"distributed_execution": false,
		"temporary_directory":   s.Config.TemporaryDirectory,
	}

	if len(s.Config.GlobalWorkDirectory) > 0 && len(s.Config.NodeList) > 0 {
		fields["distributed_execution"] = true
		fields["global_work_directory"] = s.Config.GlobalWorkDirectory
		fields["node_list"] = s.Config.NodeList

		options = append(
			options,
			scaleadpt.PolicyOpt.GlobalWorkDirectory(s.Config.GlobalWorkDirectory),
			scaleadpt.PolicyOpt.NodeList(s.Config.NodeList),
		)
	}

	s.fieldLogger.WithFields(fields).Info("Starting applying list policy")

	parser, err := filelist.ApplyPolicy(s.Config.FileSystem, options...)
	if err != nil {
		return nil, errors.Wrap(err, "(*Syncer).applyPolicy: apply policy on file system")
	}

	s.fieldLogger.WithFields(fields).Info("Finished applying list policy")

	return parser, nil
}

func (s *Syncer) writeInserts(ctx context.Context, parser *filelist.Parser) error {
	var fileData *filelist.FileData

	s.fieldLogger.Info("Starting meta data database inserts")

	inserter := s.Config.DB.NewInsertsInserter(ctx, &meda.InsertsInserterConfig{
		MaxTransactionSize: s.Config.MaxTransactionSize,
	})
	defer inserter.Close()

	err := s.fetchFileSystemPathInfo()
	if err != nil {
		return errors.Wrap(err, "(*Syncer).writeInserts")
	}

	for {
		fileData, err = parser.ParseLine()
		if err == io.EOF {
			break
		} else if err != nil {
			return errors.Wrap(err, "(*Syncer).writeInserts: parse file list line")
		}

		cleanPath, err := s.cleanPath(fileData.Path)
		if err != nil {
			return errors.Wrap(err, "(*Syncer).writeInserts")
		}

		if len(cleanPath) > meda.FilesMaxPathLength {
			s.fieldLogger.WithFields(log.Fields{
				"action":                  "skipping",
				"path":                    cleanPath,
				"path_length":             len(cleanPath),
				"max_allowed_path_length": meda.FilesMaxPathLength,
				"modification_time":       fileData.ModificationTime,
				"file_size":               int(fileData.FileSize),
			}).Warn("Skipping file because maximum allowed path length is exceeded")

			continue
		}

		err = inserter.Add(ctx, &meda.Insert{
			Path:             cleanPath,
			ModificationTime: meda.Time(fileData.ModificationTime),
			FileSize:         fileData.FileSize,
		})
		if err != nil {
			return errors.Wrap(err, "(*Syncer).writeInserts")
		}
	}

	err = inserter.Close()
	if err != nil {
		return errors.Wrap(err, "(*Syncer).writeInserts")
	}

	s.fieldLogger.WithFields(log.Fields{
		"count": inserter.Stats().InsertsCount,
	}).Info("Finished meta data database inserts")

	return nil
}

func (s *Syncer) fetchFileSystemPathInfo() error {
	mountRoot, err := s.Config.FileSystem.GetMountRoot()
	if err != nil {
		return errors.Wrap(err, "(*Syncer).fetchFileSystemPathInfo: get mount root")
	}
	snapshotDirsInfo, err := s.Config.FileSystem.GetSnapshotDirsInfo()
	if err != nil {
		return errors.Wrap(err, "(*Syncer).fetchFileSystemPathInfo: get snapshot dirs info")
	}

	basePath, err := filepath.Abs(
		filepath.Join(mountRoot, snapshotDirsInfo.Global, s.Config.SnapshotName),
	)
	if err != nil {
		return errors.Wrap(err, "(*Syncer).fetchFileSystemPathInfo: compute abs path")
	}

	s.basePath = basePath

	return nil
}

func (s *Syncer) cleanPath(path string) (string, error) {
	relPath, err := filepath.Rel(s.basePath, path)
	if err != nil {
		return "", err
	}

	return "/" + relPath, nil
}

func (s *Syncer) syncDatabase(ctx context.Context) error {
	s.fieldLogger.Info("Starting syncing the meta data database")

	var incrementalMode int
	if s.Config.SyncMode == meda.RSMIncremental {
		incrementalMode = 1
	}

	s.syncDatabaseUpdate(ctx, incrementalMode)

	s.syncDatabaseDelete(ctx, incrementalMode)

	s.syncDatabaseInsert(ctx, incrementalMode)

	s.fieldLogger.Info("Finished syncing the meta data database")

	return nil
}

func (s *Syncer) syncDatabaseUpdate(ctx context.Context, incrementalMode int) error {
	var affectedRows int64

	chunker := chunker{
		ChunkSize:      s.Config.SynchronisationChunkSize,
		NextChunkQuery: nextInsertsChunkQuery.SubstituteAll(s.Config.DB),
		DB:             s.Config.DB,
		BeginTx:        beginTxWithReadCommitted,
		ProcessChunk: func(ctx context.Context, db *meda.DB, tx *sqlx.Tx, prevID, lastID uint64) error {
			res, err := tx.ExecContext(
				ctx,
				updateQuery.SubstituteAll(s.Config.DB),
				s.Config.RunID,
				incrementalMode,
				incrementalMode,
				s.Config.RunID,
				prevID,
				lastID,
			)
			if err != nil {
				return err
			}
			num, err := res.RowsAffected()
			if err != nil {
				return err
			}
			affectedRows += num
			return nil
		},
	}
	err := chunker.Run(ctx)
	if err != nil {
		return errors.Wrap(err, "(*Syncer).syncDatabaseUpdate")
	}

	s.fieldLogger.WithFields(log.Fields{
		"affected": affectedRows,
	}).Info("Performed update of existing files in meta data database")

	return nil
}

func (s *Syncer) syncDatabaseDelete(ctx context.Context, incrementalMode int) error {
	var affectedRows int64

	chunker := chunker{
		ChunkSize:      s.Config.SynchronisationChunkSize,
		NextChunkQuery: nextFilesChunkQuery.SubstituteAll(s.Config.DB),
		DB:             s.Config.DB,
		BeginTx:        beginTxWithReadCommitted,
		ProcessChunk: func(ctx context.Context, db *meda.DB, tx *sqlx.Tx, prevID, lastID uint64) error {
			res, err := tx.ExecContext(
				ctx,
				deleteQuery.SubstituteAll(s.Config.DB),
				s.Config.RunID,
				prevID,
				lastID,
			)
			if err != nil {
				return err
			}
			num, err := res.RowsAffected()
			if err != nil {
				return err
			}
			affectedRows += num
			return nil
		},
	}
	err := chunker.Run(ctx)
	if err != nil {
		return errors.Wrap(err, "(*Syncer).syncDatabaseDelete")
	}

	s.fieldLogger.WithFields(log.Fields{
		"affected": affectedRows,
	}).Info("Performed deleting of old files in meta data database")

	return nil
}

func (s *Syncer) syncDatabaseInsert(ctx context.Context, incrementalMode int) error {
	var affectedRows int64

	chunker := chunker{
		ChunkSize:      s.Config.SynchronisationChunkSize,
		NextChunkQuery: nextInsertsChunkQuery.SubstituteAll(s.Config.DB),
		DB:             s.Config.DB,
		BeginTx:        beginTxWithReadCommitted,
		ProcessChunk: func(ctx context.Context, db *meda.DB, tx *sqlx.Tx, prevID, lastID uint64) error {
			res, err := tx.ExecContext(
				ctx,
				insertQuery.SubstituteAll(s.Config.DB),
				s.Config.RunID,
				prevID,
				lastID,
			)
			if err != nil {
				return err
			}
			num, err := res.RowsAffected()
			if err != nil {
				return err
			}
			affectedRows += num
			return nil
		},
	}
	err := chunker.Run(ctx)
	if err != nil {
		return errors.Wrap(err, "(*Syncer).syncDatabaseInsert")
	}

	s.fieldLogger.WithFields(log.Fields{
		"affected": affectedRows,
	}).Info("Performed inserting of new files in meta data database")

	return nil
}

func (s *Syncer) cleanUpDatabase(ctx context.Context) error {
	s.fieldLogger.Info("Starting cleaning up the meta data database")

	err := s.truncateInserts(ctx)
	if err != nil {
		return errors.Wrap(err, "(*Syncer).cleanUpDatabase")
	}

	s.fieldLogger.Info("Finished cleaning up the meta data database")

	return nil
}

func (s *Syncer) truncateInserts(ctx context.Context) error {
	_, err := s.execWithReadCommitted(ctx, truncateInsertsQuery.SubstituteAll(s.Config.DB))
	if err != nil {
		return errors.Wrap(err, "(*Syncer).truncateInserts")
	}

	s.fieldLogger.Info(
		"Performed truncating of inserts table in meta data database",
	)

	return nil
}

func (s *Syncer) execWithReadCommitted(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	tx, err := beginTxWithReadCommitted(ctx, s.Config.DB)
	if err != nil {
		return nil, errors.Wrap(err, "(*Syncer).execWithReadCommitted: begin transaction")
	}

	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		_ = tx.Rollback()
		return nil, errors.Wrap(err, "(*Syncer).execWithReadCommitted: exec query")
	}

	err = tx.Commit()
	if err != nil {
		return nil, errors.Wrap(err, "(*Syncer).execWithReadCommitted: commit transaction")
	}

	return res, nil
}

func alwaysRowsAffected(res sql.Result) int64 {
	num, err := res.RowsAffected()
	if err != nil {
		return -1
	}

	return num
}
