// Package medasync contains the Syncer component for synchronising the file
// system data with the meta data database.
package medasync

import (
	"context"
	"database/sql"
	"io"
	"path/filepath"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/go-errors/errors"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

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

//go:generate confions config Config

type Config struct {
	// Static config

	// MaxTransactionSize is the maximum number of commands per SQL
	// transaction.
	// After this number of commands, the transaction will be committed
	// and a new one will be begun.
	MaxTransactionSize int

	// Static Params

	Subpath             string
	TemporaryDirectory  string
	GlobalWorkDirectory string
	NodeList            []string
	Location            *time.Location `yaml:"-"`

	// Invocation dependent params

	SnapshotName string
	RunId        uint64
	SyncMode     meda.RunSyncMode

	// Dynamic objects

	DB         *meda.DB              `yaml:"-"`
	FileSystem *scaleadpt.FileSystem `yaml:"-"`
	Logger     logrus.FieldLogger    `yaml:"-"`
}

var DefaultConfig = Config{
	MaxTransactionSize: 10000,
	Location:           time.UTC,
	SyncMode:           meda.RSMFull,
}

type Syncer struct {
	Config *Config

	fieldLogger logrus.FieldLogger
	basePath    string
}

func New(config *Config) *Syncer {
	return &Syncer{
		Config: config,
	}
}

func (s *Syncer) Run(ctx context.Context) error {
	var err error

	s.fieldLogger = s.Config.Logger.WithFields(logrus.Fields{
		"run":        s.Config.RunId,
		"snapshot":   s.Config.SnapshotName,
		"filesystem": s.Config.FileSystem.GetName(),
		"subpath":    s.Config.Subpath,
		"package":    "medasync",
		"component":  "Syncer",
	})

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
	return s.cleanUpDatabase(ctx)
}

func (s *Syncer) prepareDatabase(ctx context.Context) error {
	s.fieldLogger.Info("Starting preparing the meta data database")

	err := s.truncateInserts(ctx)
	if err != nil {
		return errors.Wrap(err, 0)
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

	fields := logrus.Fields{
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
		return nil, err
	}

	s.fieldLogger.WithFields(fields).Info("Finished applying list policy")

	return parser, err
}

func (s *Syncer) writeInserts(ctx context.Context, parser *filelist.Parser) error {
	var fileData *filelist.FileData
	var medaInsert meda.Insert
	var count, txCount int

	s.fieldLogger.Info("Starting meta data database inserts")

	err := s.fetchFileSystemPathInfo()
	if err != nil {
		// TODO
		// ERRORS
		return errors.Wrap(err, 0)
	}

	tx, prepStmt, err := s.openWriteInsertsTx(ctx)
	if err != nil {
		// TODO
		// ERRORS
		return errors.Wrap(err, 0)
	}
	defer prepStmt.Close()
	defer tx.Rollback()

	for {
		fileData, err = parser.ParseLine()
		if err == io.EOF {
			break
		} else if err != nil {
			// TODO
			// ERRORS
			return errors.Wrap(err, 0)
		}

		cleanPath, err := s.cleanPath(fileData.Path)
		if err != nil {
			// TODO
			// ERRORS
			return errors.Wrap(err, 0)
		}

		if len(cleanPath) > meda.FilesMaxPathLength {
			s.fieldLogger.WithFields(logrus.Fields{
				"action":                  "skipping",
				"path":                    cleanPath,
				"path_length":             len(cleanPath),
				"max_allowed_path_length": meda.FilesMaxPathLength,
				"modification_time":       fileData.ModificationTime,
				"file_size":               int(fileData.FileSize),
			}).Warn("Skipping file because maximum allowed path length is exceeded")

			continue
		}

		medaInsert = meda.Insert{
			Path:             cleanPath,
			ModificationTime: meda.Time(fileData.ModificationTime),
			FileSize:         fileData.FileSize,
			LastSeen:         s.Config.RunId,
		}

		_, err = prepStmt.ExecContext(ctx, &medaInsert)
		if err != nil {
			// TODO
			// ERRORS
			return errors.Wrap(err, 0)
		}

		count += 1
		txCount += 1

		if txCount >= s.Config.MaxTransactionSize {
			err = s.closeInsertsInsertTx(tx, prepStmt)
			if err != nil {
				// TODO
				// ERRORS
				return errors.Wrap(err, 0)
			}

			tx, prepStmt, err = s.openWriteInsertsTx(ctx)
			if err != nil {
				// TODO
				// ERRORS
				return errors.Wrap(err, 0)
			}

			txCount = 0
		}
	}

	err = s.closeInsertsInsertTx(tx, prepStmt)
	if err != nil {
		// TODO
		// ERRORS
		return errors.Wrap(err, 0)
	}

	s.fieldLogger.WithFields(logrus.Fields{
		"count": count,
	}).Info("Finished meta data database inserts")

	return nil
}

func (s *Syncer) openWriteInsertsTx(ctx context.Context) (*sqlx.Tx, *sqlx.NamedStmt, error) {
	tx, err := s.Config.DB.BeginTxx(ctx, nil)
	if err != nil {
		return nil, nil, err
	}

	prepStmt, err := s.Config.DB.InsertsPrepareInsert(ctx, tx)
	if err != nil {
		_ = tx.Rollback()
		return nil, nil, err
	}

	return tx, prepStmt, nil
}

func (s *Syncer) closeInsertsInsertTx(tx *sqlx.Tx, prepStmt *sqlx.NamedStmt) error {
	err := prepStmt.Close()
	if err != nil {
		// Try to commit
		_ = tx.Commit()
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (s *Syncer) fetchFileSystemPathInfo() error {
	mountRoot, err := s.Config.FileSystem.GetMountRoot()
	if err != nil {
		return err
	}
	snapshotDirsInfo, err := s.Config.FileSystem.GetSnapshotDirsInfo()
	if err != nil {
		return err
	}

	basePath, err := filepath.Abs(
		filepath.Join(mountRoot, snapshotDirsInfo.Global, s.Config.SnapshotName),
	)
	if err != nil {
		return err
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

	res, err := s.execWithReadCommitted(
		ctx, updateQuery.SubstituteAll(s.Config.DB), s.Config.RunId, incrementalMode,
		incrementalMode, s.Config.RunId,
	)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	s.fieldLogger.WithFields(logrus.Fields{
		"affected": alwaysRowsAffected(res),
	}).Info("Performed update of existing files in meta data database")

	res, err = s.execWithReadCommitted(ctx, deleteQuery.SubstituteAll(s.Config.DB), s.Config.RunId)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	s.fieldLogger.WithFields(logrus.Fields{
		"affected": alwaysRowsAffected(res),
	}).Info("Performed deleting of old files in meta data database")

	res, err = s.execWithReadCommitted(ctx, insertQuery.SubstituteAll(s.Config.DB), s.Config.RunId)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	s.fieldLogger.WithFields(logrus.Fields{
		"affected": alwaysRowsAffected(res),
	}).Info("Performed copying of new files in meta data database")

	s.fieldLogger.Info("Finished syncing the meta data database")

	return nil
}

func (s *Syncer) cleanUpDatabase(ctx context.Context) error {
	s.fieldLogger.Info("Starting cleaning up the meta data database")

	err := s.truncateInserts(ctx)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	s.fieldLogger.Info("Finished cleaning up the meta data database")

	return nil
}

func (s *Syncer) truncateInserts(ctx context.Context) error {
	s.fieldLogger.Info(
		"Starting truncating inserts table in meta data database",
	)

	_, err := s.execWithReadCommitted(ctx, truncateInsertsQuery.SubstituteAll(s.Config.DB))
	if err != nil {
		return errors.Wrap(err, 0)
	}

	s.fieldLogger.Info(
		"Finished truncating inserts table in meta data database",
	)

	return nil
}

func (s *Syncer) execWithReadCommitted(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	tx, err := s.Config.DB.BeginTxx(ctx, txOptionsReadCommitted)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		_ = tx.Commit()
		return nil, errors.Wrap(err, 0)
	}

	err = tx.Commit()
	if err != nil {
		return nil, errors.Wrap(err, 0)
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
