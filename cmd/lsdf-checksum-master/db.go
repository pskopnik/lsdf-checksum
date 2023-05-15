package main

import (
	"context"
	"fmt"

	"github.com/apex/log"
	_ "github.com/go-sql-driver/mysql"

	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

func prepareDB(ctx context.Context, logger log.Interface, userConfig *meda.Config) (*meda.DB, error) {
	config := meda.DefaultConfig.
		Clone().
		Merge(userConfig).
		Merge(&meda.Config{})

	db, err := meda.Open(config)
	if err != nil {
		logger.WithError(err).WithFields(log.Fields{
			"driver":           config.Driver,
			"data_source_name": meda.RedactDSN(config.DataSourceName),
			"table_prefix":     config.TablePrefix,
		}).Error("Database open returned error")

		return nil, err
	}

	version, err := db.GetVersion(ctx)
	if err != nil {
		logger.WithError(err).WithFields(log.Fields{
			"driver":           config.Driver,
			"data_source_name": meda.RedactDSN(config.DataSourceName),
			"table_prefix":     config.TablePrefix,
		}).Error("Encountered error while querying version from database")

		return nil, err
	}
	logger.WithFields(log.Fields{
		"driver":           config.Driver,
		"data_source_name": meda.RedactDSN(config.DataSourceName),
		"table_prefix":     config.TablePrefix,
		"version":          version,
	}).Info("Queried version from mysql database server")

	err = db.Migrate(ctx)
	if err != nil {
		logger.WithError(err).WithFields(log.Fields{
			"driver":           config.Driver,
			"data_source_name": meda.RedactDSN(config.DataSourceName),
			"table_prefix":     config.TablePrefix,
		}).Error("Encountered error while performing meda migrations")

		return nil, err
	}

	return db, nil
}

func openDB(ctx context.Context, userConfig *meda.Config) (*meda.DB, error) {
	config := meda.DefaultConfig.
		Clone().
		Merge(userConfig).
		Merge(&meda.Config{})

	db, err := meda.Open(config)
	if err != nil {
		return nil, fmt.Errorf(
			"openDB %s: %w",
			meda.RedactDSN(config.DataSourceName),
			err,
		)
	}

	_, err = db.GetVersion(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"openDB %s: %w",
			meda.RedactDSN(config.DataSourceName),
			err,
		)
	}

	return db, nil
}
