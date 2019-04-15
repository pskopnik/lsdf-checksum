package main

import (
	"context"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
)

var CollectJoinMethod = Method{
	Name:                 "collectjoin",
	SupportsBatching:     true,
	SupportsTransactions: false,
	MaxBatchSize:         32767,
	CreateRunnerFunc: func(runnerConfig *RunnerConfig) Runner {
		incorporater := &CollectJoinMethodIncorporater{
			runnerConfig: runnerConfig,
		}

		return &RunnerBase{
			RunnerConfig:   runnerConfig,
			ProcessingSize: runnerConfig.BatchSize,
			CreateProcessor: func(nestedRunnerConfig *RunnerConfig) ProcessBatcher {
				return &CollectJoinMethodProcessor{
					runnerConfig: nestedRunnerConfig,
					incorporater: incorporater,
				}
			},
			Prepare:  incorporater.Prepare,
			Finalise: incorporater.Finalise,
			TearDown: incorporater.TearDown,
		}
	},
}

type CollectJoinMethodIncorporater struct {
	runnerConfig *RunnerConfig
	replacer     *strings.Replacer
}

func (c *CollectJoinMethodIncorporater) Prepare(ctx context.Context) error {
	var err error

	c.replacer = c.createReplacer()

	err = c.calculatedChecksumsCreateTable(ctx, c.runnerConfig.DB)
	if err != nil {
		return err
	}

	err = c.checksumWarningsCreateTable(ctx, c.runnerConfig.DB)
	if err != nil {
		_ = c.TearDown(ctx)
		return err
	}

	return nil
}

func (c *CollectJoinMethodIncorporater) createReplacer() *strings.Replacer {
	return strings.NewReplacer(
		"{CHECKSUM_WARNINGS}", c.checksumWarningsTableName(),
		"{CALCULATED_CHECKSUMS}", c.calculatedChecksumsTableName(),
	)
}

func (c *CollectJoinMethodIncorporater) substituteAll(query GenericQuery) string {
	return c.replacer.Replace(string(query.SubstituteAll(c.runnerConfig.DB)))
}

const writeChecksumWarningsQuery = GenericQuery(`
	INSERT INTO {CHECKSUM_WARNINGS}
		(
			file_id,
			path,
			modification_time,
			file_size,
			expected_checksum,
			actual_checksum,
			discovered,
			last_read,
			created
		)
		SELECT
			{CALCULATED_CHECKSUMS}.file_id,
			{FILES}.path,
			{FILES}.modification_time,
			{FILES}.file_size,
			{FILES}.checksum,
			{CALCULATED_CHECKSUMS}.checksum,
			?,
			{FILES}.last_read,
			NOW()
		FROM {CALCULATED_CHECKSUMS}
		LEFT JOIN {FILES}
			ON {CALCULATED_CHECKSUMS}.file_id = {FILES}.id
		WHERE {FILES}.to_be_compared = 1
			AND
				{FILES}.checksum <> {CALCULATED_CHECKSUMS}.checksum
	;
`)

const updateFilesQuery = GenericQuery(`
	UPDATE {FILES}
		RIGHT JOIN {CALCULATED_CHECKSUMS}
			ON {CALCULATED_CHECKSUMS}.file_id = {FILES}.id
		SET
			{FILES}.to_be_read = 0,
			{FILES}.to_be_compared = 0,
			{FILES}.checksum = {CALCULATED_CHECKSUMS}.checksum,
			{FILES}.last_read = ?
	;
`)

func (c *CollectJoinMethodIncorporater) Finalise(ctx context.Context) error {
	var err error

	_, err = c.runnerConfig.DB.ExecContext(ctx, c.substituteAll(writeChecksumWarningsQuery), c.runnerConfig.RunID)
	if err != nil {
		return err
	}

	_, err = c.runnerConfig.DB.ExecContext(ctx, c.substituteAll(updateFilesQuery), c.runnerConfig.RunID)
	if err != nil {
		return err
	}

	return nil
}

func (c *CollectJoinMethodIncorporater) TearDown(ctx context.Context) error {
	var err error

	err = c.checksumWarningsDropTable(ctx, c.runnerConfig.DB)
	if err != nil {
		_ = c.calculatedChecksumsDropTable(ctx, c.runnerConfig.DB)
		return err
	}

	err = c.calculatedChecksumsDropTable(ctx, c.runnerConfig.DB)
	if err != nil {
		return err
	}

	return nil
}

const calculatedChecksumsTableNameBase = "calculated_checksums"

func (c *CollectJoinMethodIncorporater) calculatedChecksumsTableName() string {
	return c.runnerConfig.DB.Config.TablePrefix + calculatedChecksumsTableNameBase
}

const calculatedChecksumsCreateTableQuery = GenericQuery(`
	CREATE TABLE IF NOT EXISTS {CALCULATED_CHECKSUMS} (
		file_id bigint(20) unsigned NOT NULL,
		checksum varbinary(64) NOT NULL,
		PRIMARY KEY (file_id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`)

func (c *CollectJoinMethodIncorporater) calculatedChecksumsCreateTable(ctx context.Context, execer sqlx.ExecerContext) error {
	_, err := execer.ExecContext(ctx, c.substituteAll(calculatedChecksumsCreateTableQuery))
	return err
}

const calculatedChecksumsDropTableQuery = GenericQuery(`
	DROP TABLE IF EXISTS {CALCULATED_CHECKSUMS};
`)

func (c *CollectJoinMethodIncorporater) calculatedChecksumsDropTable(ctx context.Context, execer sqlx.ExecerContext) error {
	_, err := execer.ExecContext(ctx, c.substituteAll(calculatedChecksumsDropTableQuery))
	return err
}

const checksumWarningsTableNameBase = "checksum_warnings"

func (c *CollectJoinMethodIncorporater) checksumWarningsTableName() string {
	return c.runnerConfig.DB.Config.TablePrefix + checksumWarningsTableNameBase
}

const checksumWarningsCreateTableQuery = GenericQuery(`
	CREATE TABLE IF NOT EXISTS {CHECKSUM_WARNINGS} (
		id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
		file_id bigint(20) unsigned NOT NULL,
		path varbinary(4096) NOT NULL,
		modification_time datetime(6) NOT NULL,
		file_size bigint(20) unsigned NOT NULL,
		expected_checksum varbinary(64) NOT NULL,
		actual_checksum varbinary(64) NOT NULL,
		discovered bigint(20) unsigned NOT NULL,
		last_read bigint(20) unsigned NOT NULL,
		created datetime(6) NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`)

func (c *CollectJoinMethodIncorporater) checksumWarningsCreateTable(ctx context.Context, execer sqlx.ExecerContext) error {
	_, err := execer.ExecContext(ctx, c.substituteAll(checksumWarningsCreateTableQuery))
	return err
}

const checksumWarningsDropTableQuery = GenericQuery(`
	DROP TABLE IF EXISTS {CHECKSUM_WARNINGS};
`)

func (c *CollectJoinMethodIncorporater) checksumWarningsDropTable(ctx context.Context, execer sqlx.ExecerContext) error {
	_, err := execer.ExecContext(ctx, c.substituteAll(checksumWarningsDropTableQuery))
	return err
}

var _ ProcessBatcher = &CollectJoinMethodProcessor{}

type CollectJoinMethodProcessor struct {
	runnerConfig *RunnerConfig
	incorporater *CollectJoinMethodIncorporater
}

func (c *CollectJoinMethodProcessor) ProcessBatch(ctx context.Context, batch Batch) error {
	var err error
	calculatedChecksums := batch.Checksums

	tx, err := c.runnerConfig.DB.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	insert := c.buildInsertBase(tx)

	for id, checksum := range calculatedChecksums {
		insert = insert.Values(
			id,
			checksum,
		)
	}

	_, err = insert.ExecContext(ctx)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (_ *CollectJoinMethodProcessor) Finalise(_ context.Context) error {
	return nil
}

func (c *CollectJoinMethodProcessor) buildInsertBase(runner squirrel.BaseRunner) squirrel.InsertBuilder {
	return squirrel.Insert(c.incorporater.calculatedChecksumsTableName()).
		Columns(
			"file_id",
			"checksum",
		).
		PlaceholderFormat(squirrel.Question).
		RunWith(runner)
}
