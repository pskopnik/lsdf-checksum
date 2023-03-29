package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"

	"gopkg.in/yaml.v3"
	_ "github.com/go-sql-driver/mysql"

	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt"
)

func determineFSSubpath(filesystemName, rootDir string) (string, string, error) {
	fs := scaleadpt.OpenFileSystem(filesystemName)
	mountRoot, err := fs.GetMountRoot()
	if err != nil {
		return "", "", fmt.Errorf("determineFSSubpath: %w", err)
	}

	subpath, err := filepath.Rel(mountRoot, rootDir)
	if err != nil {
		return "", "", fmt.Errorf("determineFSSubpath: %w", err)
	}

	return subpath, mountRoot, nil
}

func lookupBin(name string, extraPath string) (string, error) {
	for _, dir := range filepath.SplitList(extraPath) {
		binPath := filepath.Join(dir, name)

		info, err := os.Stat(binPath)
		if os.IsNotExist(err) {
			continue
		} else if err != nil {
			return "", fmt.Errorf("lookupBin: stat on %s: %w", binPath, err)
		}

		if info.Mode().IsRegular() {
			return binPath, nil
		}
	}

	return exec.LookPath(name)
}

type configSupplements struct {
	ChecksumBin    string
	ChtreeBin      string
	GentreeBin     string
	FilesystemRoot string
	ChecksumConfig checksumConfig
	DataSourceName string
	Driver         string
}

func prepareConfigSupplements(config *Config) (*configSupplements, error) {
	var cs configSupplements
	var err error

	if !config.Gentree.Skip {
		cs.GentreeBin, err = lookupBin("gentree", config.BinPath)
		if err != nil {
			return nil, fmt.Errorf("prepareConfigSupplements: %w", err)
		}
	}
	cs.ChtreeBin, err = lookupBin("chtree", config.BinPath)
	if err != nil {
		return nil, fmt.Errorf("prepareConfigSupplements: %w", err)
	}
	cs.ChecksumBin, err = lookupBin("lsdf-checksum-master", config.BinPath)
	if err != nil {
		return nil, fmt.Errorf("prepareConfigSupplements: %w", err)
	}

	subpath, filesystemRoot, err := determineFSSubpath(config.FilesystemName, config.RootDir)
	if err != nil {
		return nil, fmt.Errorf("prepareConfigSupplements: %w", err)
	}

	cs.FilesystemRoot = filesystemRoot

	cs.ChecksumConfig = checksumConfig{
		BaseConfigPath:    config.Checksum.BaseConfig,
		FileSystemName:    config.FilesystemName,
		FileSystemSubpath: subpath,
		CoverDir:          config.Checksum.CoverDir,
	}

	if config.ClearDatabase {
		m, err := loadConfigFileIntoMap(config.Checksum.BaseConfig)
		if err != nil {
			return nil, fmt.Errorf("prepareConfigSupplements: %w", err)
		}

		cs.Driver = m.Get("db.driver").Str("mysql")
		cs.DataSourceName = m.Get("db.datasourcename").Str()
		if cs.DataSourceName == "" {
			return nil, fmt.Errorf("prepareConfigSupplements: no db.datasourcename available in checksum config")
		}
	}

	return &cs, nil
}

func clearDatabase(driver, dataSourceName string) error {
	db, err := sql.Open(driver, dataSourceName)
	if err != nil {
		return fmt.Errorf("clearDatabase: open database: %w", err)
	}
	defer db.Close()

	ctx := context.Background()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("clearDatabase: begin txn: %w", err)
	}
	defer tx.Rollback()

	var databaseName sql.NullString
	row := tx.QueryRowContext(ctx, "SELECT DATABASE();")
	err = row.Scan(&databaseName)
	if err != nil {
		return fmt.Errorf("clearDatabase: retrieve: %w", err)
	}
	if !databaseName.Valid {
		return fmt.Errorf("clearDatabase: No database used in dataSourceName")
	}

	var tableNames []string
	rows, err := tx.QueryContext(ctx, "SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA=?;", databaseName.String)
	if err != nil {
		return fmt.Errorf("clearDatabase: query table names: %w", err)
	}
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			break
		}
		tableNames = append(tableNames, tableName)
	}
	if err != nil {
		_ = rows.Close()
		return fmt.Errorf("clearDatabase: iterate over rows: %w", err)
	}
	err = rows.Close()
	if err != nil {
		return fmt.Errorf("clearDatabase: close rows iterator: %w", err)
	}

	_, err = tx.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 0;")
	if err != nil {
		return fmt.Errorf("clearDatabase: disable foreign key checks: %w", err)
	}

	for _, name := range tableNames {
		log.Println("dropping table", name)
		_, err = tx.ExecContext(ctx, "DROP TABLE IF EXISTS "+name+";")
		if err != nil {
			return fmt.Errorf("clearDatabase: drop table %s: %w", name, err)
		}
	}

	_, err = tx.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 1;");
	if err != nil {
		return fmt.Errorf("clearDatabase: enable foreign key checks: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("clearDatabase: commit txn: %w", err)
	}

	err = db.Close()
	if err != nil {
		return fmt.Errorf("clearDatabase: close database: %w", err)
	}

	return nil
}

func PerformTest(config *Config) error {
	cs, err := prepareConfigSupplements(config)
	if err != nil {
		log.Printf("gentree returned error, aborting: %v", err)
		return err
	}

	if config.ClearDatabase {
		log.Println("Starting clearing of database")
		err = clearDatabase(cs.Driver, cs.DataSourceName)
		log.Println("Finished clearing of database")
		if err != nil {
			log.Printf("clearing of database returned error, aborting: %v", err)
			return err
		}
	}

	log.Println("Starting initialisation (gentree, initial checksum run)")
	err = performTestInitialisation(config, cs)
	log.Println("Finished initialisation")
	if err != nil {
		log.Printf("Error during initialisation: %v", err)
		return err
	}

	for i, step := range config.Steps {
		stepName := fmt.Sprintf("#%d", i+1)

		log.Printf("Starting step %s", stepName)
		err = performTestStep(stepName, config, step, cs)
		log.Printf("Finished step %s", stepName)
		if err != nil {
			log.Printf("Error during step %s, aborting: %v", stepName, err)
			return err
		}
	}

	return nil
}

func assertWarningsMatchChanges(warningsRootDir string, warnings []checksumWarning, changes []changedFile) error {
	corruptedFiles := make(map[string]struct{})
	for _, change := range changes {
		if !change.CorruptedOnly {
			continue
		}
		relPath, err := filepath.Rel(warningsRootDir, change.Path)
		if err != nil {
			return fmt.Errorf("assertWarningsMatchChanges: relative path from changes: %w", err)
		}
		fsPath := filepath.Join("/", relPath)
		if _, ok := corruptedFiles[fsPath]; ok {
			return fmt.Errorf("assertWarningsMatchChanges: Duplicate path in changes: %s", fsPath)
		}
		corruptedFiles[fsPath] = struct{}{}
	}

	warnedFiles := make(map[string]struct{})
	for _, warning := range warnings {
		if _, ok := warnedFiles[warning.Path]; ok {
			return fmt.Errorf("assertWarningsMatchChanges: Duplicate path in warnings: %s", warning.Path)
		}
		warnedFiles[warning.Path] = struct{}{}
	}

	for path := range warnedFiles {
		if _, ok := corruptedFiles[path]; !ok {
			return fmt.Errorf("assertWarningsMatchChanges: warning not in changes: %s", path)
		}
		delete(corruptedFiles, path)
	}
	for path := range corruptedFiles {
		return fmt.Errorf("assertWarningsMatchChanges: change not in warnings: %s", path)
	}

	return nil
}

func performTestInitialisation(config *Config, cs *configSupplements) error {
	var err error

	if !config.Gentree.Skip {
		log.Println("Starting initial gentree")
		err = runGentree(cs.GentreeBin, gentreeConfig{
			BaseConfigPath: config.Gentree.BaseConfig,
			RootDir:        config.RootDir,
		})
		log.Println("Finished initial gentree")
		if err != nil {
			log.Printf("gentree returned error, aborting: %v", err)
			return err
		}
	} else {
		log.Println("Skipping initial gentree as per configuration")
	}

	log.Println("Starting initial checksum full run")
	var logPath string
	if config.Checksum.LogDir != "" {
		logPath = filepath.Join(config.Checksum.LogDir, "checksum.log.initial.full.log")
	}
	var raceLogPath string
	if config.Checksum.RaceLogDir != "" {
		raceLogPath = filepath.Join(config.Checksum.RaceLogDir, "checksum.race_log.initial.full")
	}
	err = runChecksumRun(cs.ChecksumBin, checksumRunConfig{
		ChecksumConfig: cs.ChecksumConfig,
		RunMode:        CRSM_FULL,
		LogPath:        logPath,
		RaceLogPath:    raceLogPath,
	})
	log.Println("Finished initial checksum full run")
	if err != nil {
		log.Printf("Checksum full run returned error, aborting: %v", err)
		return err
	}

	// TODO: Check that run was created and finished

	warnings, err := runChecksumWarnings(cs.ChecksumBin, checksumWarningsConfig{
		ChecksumConfig: cs.ChecksumConfig,
		OnlyLastRun:    true,
	})
	if err != nil {
		log.Printf("checksum warnings returned error, aborting: %v", err)
		return err
	}

	if len(warnings) > 0 {
		return fmt.Errorf("checksum incremental run returned warnings")
	}

	return nil
}

func corruptedOnlyCount(changes []changedFile) (count int) {
	// Does not perform deduplication on files... but those are not supposed
	// to occur anyway.
	for _, change := range changes {
		if change.CorruptedOnly {
			count++
		}
	}

	return
}

func performTestStep(name string, config *Config, step ConfigStep, cs *configSupplements) error {
	var err error

	log.Println("Starting chtree")
	changes, err := runChtree(cs.ChtreeBin, buildChtreeConfig(config.RootDir, step.ChangeLikelihood, step.CorruptLikelihood))
	log.Println("Finished chtree")
	if err != nil {
		log.Printf("chtree returned error, aborting: %v", err)
		return err
	}

	if step.CorruptLikelihood > 0 && corruptedOnlyCount(changes) == 0 {
		log.Printf("WARNING: chtree did not produce any corruptions although corrupt likelihood (is %f) > 0", step.CorruptLikelihood)
	}

	if step.PerformIncrementalRun {
		log.Println("Starting checksum incremental run")
		var logPath string
		if config.Checksum.LogDir != "" {
			logPath = filepath.Join(config.Checksum.LogDir, "checksum.log.step"+name+".incremental.log")
		}
		var raceLogPath string
		if config.Checksum.RaceLogDir != "" {
			raceLogPath = filepath.Join(config.Checksum.RaceLogDir, "checksum.race_log.step"+name+".incremental")
		}
		err = runChecksumRun(cs.ChecksumBin, checksumRunConfig{
			ChecksumConfig: cs.ChecksumConfig,
			RunMode:        CRSM_INCREMENTAL,
			LogPath:        logPath,
			RaceLogPath:    raceLogPath,
		})
		log.Println("Finished checksum incremental run")
		if err != nil {
			log.Printf("Checksum incremental run returned error, aborting: %v", err)
			return err
		}

		// TODO: Check that run was created and finished

		warnings, err := runChecksumWarnings(cs.ChecksumBin, checksumWarningsConfig{
			ChecksumConfig: cs.ChecksumConfig,
			OnlyLastRun:    true,
		})
		if err != nil {
			log.Printf("checksum warnings returned error, aborting: %v", err)
			return err
		}

		if len(warnings) > 0 {
			return fmt.Errorf("checksum incremental run returned warnings")
		}
	}

	log.Println("Starting checksum full run")
	var logPath string
	if config.Checksum.LogDir != "" {
		logPath = filepath.Join(config.Checksum.LogDir, "checksum.log.step"+name+".incremental.log")
	}
	var raceLogPath string
	if config.Checksum.RaceLogDir != "" {
		raceLogPath = filepath.Join(config.Checksum.RaceLogDir, "checksum.race_log.step"+name+".incremental")
	}
	err = runChecksumRun(cs.ChecksumBin, checksumRunConfig{
		ChecksumConfig: cs.ChecksumConfig,
		RunMode:        CRSM_FULL,
		LogPath:        logPath,
		RaceLogPath:    raceLogPath,
	})
	log.Println("Finished checksum full run")
	if err != nil {
		log.Printf("Checksum full run returned error, aborting: %v", err)
		return err
	}

	// TODO: Check that run was created and finished

	warnings, err := runChecksumWarnings(cs.ChecksumBin, checksumWarningsConfig{
		ChecksumConfig: cs.ChecksumConfig,
		OnlyLastRun:    true,
	})
	if err != nil {
		log.Printf("checksum warnings returned error, aborting: %v", err)
		return err
	}

	err = assertWarningsMatchChanges(cs.FilesystemRoot, warnings, changes)
	if err != nil {
		log.Printf("Mismatch in checksum warnings: %v", err)
		return err
	}

	return nil
}

type Config struct {
	FilesystemName string         `yaml:"filesystem_name"`
	RootDir        string         `yaml:"root_dir"`
	BinPath        string         `yaml:"bin_path"`
	ClearDatabase  bool           `yaml:"clear_database"`
	Gentree        ConfigGentree  `yaml:"gentree"`
	Checksum       ConfigChecksum `yaml:"checksumming"`
	Steps          []ConfigStep   `yaml:"steps"`
}

type ConfigGentree struct {
	BaseConfig string `yaml:"base_config"`
	Skip       bool   `yaml:"skip,omitempty"`
}

type ConfigChecksum struct {
	BaseConfig string `yaml:"base_config"`
	LogDir     string `yaml:"log_dir"`
	RaceLogDir string `yaml:"race_log_dir"`
	CoverDir   string `yaml:"cover_dir"`
}

type ConfigStep struct {
	ChangeLikelihood      float64 `yaml:"change_likelihood"`
	CorruptLikelihood     float64 `yaml:"corrupt_likelihood"`
	PerformIncrementalRun bool    `yaml:"perform_incremental_run,omitempty"`
}

func readConfig(path string) (*Config, error) {
	configFile, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer configFile.Close()

	config := &Config{}

	dec := yaml.NewDecoder(configFile)
	err = dec.Decode(config)
	if err != nil {
		return nil, err
	}

	return config, nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage:", os.Args[0], "<config.yaml>")
		os.Exit(1)
	}

	config, err := readConfig(os.Args[1])
	if err != nil {
		panic(err)
	}

	err = PerformTest(config)
	if err != nil {
		panic(err)
	}
}
