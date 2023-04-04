package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime/pprof"

	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/yaml.v3"

	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt/filelist"
)

//go:generate confions config Config

type Config struct {
	DB             meda.Config
	FileListPath   string
	Inserter       meda.InsertsInserterConfig
	StopAfterNRows int
	CPUProfile     string
}

var DefaultConfig = &Config{}

func readConfig(path string) (*Config, error) {
	configFile, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer configFile.Close()

	inpConfig := &Config{}

	dec := yaml.NewDecoder(configFile)
	err = dec.Decode(inpConfig)
	if err != nil {
		return nil, err
	}

	config := DefaultConfig.Clone().
		Merge(inpConfig)

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

	if len(config.CPUProfile) > 0 {
		f, err := os.Create(config.CPUProfile)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	ctx := context.Background()

	db, err := meda.Open(
		meda.DefaultConfig.
			Clone().
			Merge(&config.DB),
	)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Migrate(ctx)
	if err != nil {
		panic(err)
	}

	err = insertFileList(ctx, config, db)
	if err != nil {
		panic(err)
	}
}

func insertFileList(ctx context.Context, config *Config, db *meda.DB) error {
	f, err := os.Open(config.FileListPath)
	if err != nil {
		return err
	}
	defer f.Close()

	parser := filelist.NewParser(f)

	inserter := db.NewInsertsInserter(ctx, *meda.InsertsInserterDefaultConfig.
		Clone().
		Merge(&config.Inserter),
	)
	defer inserter.Close(context.Background())

	var count int

	for {
		var fileData filelist.FileData
		err := parser.ParseLine(&fileData)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		inserter.Add(ctx, &meda.Insert{
			Path:             fileData.Path,
			FileSize:         fileData.FileSize,
			ModificationTime: meda.Time(fileData.ModificationTime),
		})

		count++
		if config.StopAfterNRows != 0 && count >= config.StopAfterNRows {
			break
		}
	}

	err = inserter.Close(ctx)
	if err != nil {
		return err
	}

	return nil
}
