package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/yaml.v3"

	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt/filelist"
)

type Config struct {
	DB                 meda.Config
	FileListPath       string
	MaxTransactionSize int
	StopAfterNRows     int
}

var DefaultConfig = &Config{
	DB:                 *meda.DefaultConfig,
	MaxTransactionSize: 10000,
}

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

	ctx := context.Background()

	db, err := meda.Open(&config.DB)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Migrate(ctx)
	if err != nil {
		panic(err)
	}

	insertFileList(ctx, config, db)
}

func insertFileList(ctx context.Context, config *Config, db *meda.DB) error {
	f, err := os.Open(config.FileListPath)
	if err != nil {
		return err
	}
	defer f.Close()

	bufferedF := bufio.NewReader(f)

	parser := filelist.NewParser(bufferedF)

	inserter := db.NewInsertsInserter(ctx, &meda.InsertsInserterConfig{
		MaxTransactionSize: config.MaxTransactionSize,
	})
	defer inserter.Close()

	var count int

	for {
		fileData, err := parser.ParseLine()
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

	err = inserter.Close()
	if err != nil {
		return err
	}

	return nil
}
