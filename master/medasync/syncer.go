// Package medasync contains the Syncer utility for synchronising the file
// system data with the meta data database.
package medasync

import (
	"database/sql"
	"io"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt"
	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt/filelist"
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

	GlobalWorkDirectory string
	NodeList            []string
	Subpath             string
	Location            *time.Location

	// Invokation dependent params

	SnapshotName string
	RunId        int

	// Dynamic objects

	DB         *sqlx.DB
	FileSystem *scaleadpt.FileSystem
	Logger     logrus.FieldLogger
}

var DefaultConfig = &Config{
	MaxTransactionSize: 10000,
	Location:           time.Local,
}

type Syncer struct {
	Config      *Config
	fieldLogger logrus.FieldLogger
}

func New(config *Config) *Syncer {
	return &Syncer{
		Config: config,
	}
}

func (s *Syncer) Run() error {
	var err error

	s.fieldLogger = s.Config.Logger.WithFields(logrus.Fields{
		"run":        s.Config.RunId,
		"snapshot":   s.Config.SnapshotName,
		"filesystem": "",
		"module":     "medasync",
	})

	parser, err := s.applyPolicy()
	if err != nil {
		return err
	}

	parser.Loc = s.Config.Location

	err = s.writeInserts(&parser.Parser)
	if err != nil {
		_ = parser.Close()
		return err
	}

	err = parser.Close()
	if err != nil {
		return err
	}

	err = s.syncDatabase()
	if err != nil {
		return err
	}

	return nil
}

func (s *Syncer) applyPolicy() (*filelist.CloseParser, error) {
	options := []scaleadpt.PolicyOptioner{
		scaleadpt.PolicyOpt.SnapshotName(s.Config.SnapshotName),
		scaleadpt.PolicyOpt.Subpath(s.Config.Subpath),
	}

	fields := logrus.Fields{
		"distributed_execution": false,
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

func (s *Syncer) writeInserts(parser *filelist.Parser) error {
	var fileData *filelist.FileData
	var medaFile meda.Insert
	var count, txCount int

	s.fieldLogger.Info("Starting meta data database inserts")

	tx, prepStmt, err := s.openWriteInsertsTx()
	defer tx.Rollback()

	for {
		fileData, err = parser.ParseLine()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		medaFile = meda.Insert{
			Path:             s.cleanPath(fileData.Path),
			ModificationTime: fileData.ModificationTime,
			FileSize:         int(fileData.FileSize),
			LastSeen:         s.Config.RunId,
		}

		_, err = prepStmt.Exec(&medaFile)
		if err != nil {
			return err
		}

		count += 1
		txCount += 1

		if txCount >= s.Config.MaxTransactionSize {
			err = s.closeWriteInsertsTx(tx, prepStmt)
			if err != nil {
				return err
			}

			tx, prepStmt, err = s.openWriteInsertsTx()
			if err != nil {
				return err
			}

			txCount = 0
		}
	}

	err = s.closeWriteInsertsTx(tx, prepStmt)
	if err != nil {
		return err
	}

	s.fieldLogger.WithFields(logrus.Fields{
		"count": count,
	}).Info("Finished meta data database inserts")

	return nil
}

func (s *Syncer) openWriteInsertsTx() (*sqlx.Tx, *sqlx.NamedStmt, error) {
	tx, err := s.Config.DB.Beginx()
	if err != nil {
		return nil, nil, err
	}

	prepStmt, err := meda.InsertsPrepareInsert(tx)
	if err != nil {
		_ = tx.Rollback()
		return nil, nil, err
	}

	return tx, prepStmt, nil
}

func (s *Syncer) closeWriteInsertsTx(tx *sqlx.Tx, prepStmt *sqlx.NamedStmt) error {
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

func (s *Syncer) cleanPath(path string) string {
	// TODO
	return path
}

func (s *Syncer) syncDatabase() error {
	s.fieldLogger.Info("Starting syncing the meta data database")

	res, err := s.Config.DB.Exec(updateQuery.get(), s.Config.RunId)
	if err != nil {
		return err
	}

	s.fieldLogger.WithFields(logrus.Fields{
		"affected": alwaysRowsAffected(res),
	}).Info("Performed update of existing files in meta data database")

	res, err = s.Config.DB.Exec(insertQuery.get(), s.Config.RunId)
	if err != nil {
		return err
	}

	s.fieldLogger.WithFields(logrus.Fields{
		"affected": alwaysRowsAffected(res),
	}).Info("Performed copying of new files in meta data database")

	res, err = s.Config.DB.Exec(deleteQuery.get(), s.Config.RunId)
	if err != nil {
		return err
	}

	s.fieldLogger.WithFields(logrus.Fields{
		"affected": alwaysRowsAffected(res),
	}).Info("Performed deleting of old files in meta data database")

	res, err = s.Config.DB.Exec(cleanInsertsQuery.get(), s.Config.RunId)
	if err != nil {
		return err
	}

	s.fieldLogger.WithFields(logrus.Fields{
		"affected": alwaysRowsAffected(res),
	}).Info("Performed cleaning of the inserts table")

	s.fieldLogger.Info("Finished syncing the meta data database")

	return nil
}

func alwaysRowsAffected(res sql.Result) int64 {
	num, err := res.RowsAffected()
	if err != nil {
		return -1
	}

	return num
}
