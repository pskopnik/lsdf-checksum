package main

import (
	"context"

	"github.com/apex/log"
	_ "github.com/go-sql-driver/mysql"

	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

func prepareDB(ctx context.Context, logger log.Interface, config *meda.Config) (*meda.DB, error) {
	dbConfig := meda.DefaultConfig.
		Clone().
		Merge(config).
		Merge(&meda.Config{})

	db, err := meda.Open(dbConfig)
	if err != nil {
		logger.WithError(err).WithFields(log.Fields{
			"driver":           dbConfig.Driver,
			"data_source_name": meda.RedactDSN(dbConfig.DataSourceName),
			"table_prefix":     dbConfig.TablePrefix,
		}).Error("Database open returned error")

		return nil, err
	}

	version, err := db.GetVersion(ctx)
	if err != nil {
		logger.WithError(err).WithFields(log.Fields{
			"driver":           dbConfig.Driver,
			"data_source_name": meda.RedactDSN(dbConfig.DataSourceName),
			"table_prefix":     dbConfig.TablePrefix,
		}).Error("Encountered error while querying version from database")

		return nil, err
	}
	logger.WithFields(log.Fields{
		"driver":           dbConfig.Driver,
		"data_source_name": meda.RedactDSN(dbConfig.DataSourceName),
		"table_prefix":     dbConfig.TablePrefix,
		"version":          version,
	}).Info("Queried version from mysql database server")

	err = db.Migrate(ctx)
	if err != nil {
		logger.WithError(err).WithFields(log.Fields{
			"driver":           dbConfig.Driver,
			"data_source_name": meda.RedactDSN(dbConfig.DataSourceName),
			"table_prefix":     dbConfig.TablePrefix,
		}).Error("Encountered error while performing meda migrations")

		return nil, err
	}

	return db, nil
}
