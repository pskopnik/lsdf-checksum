package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"gopkg.in/yaml.v3"

	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

type Config struct {
	DB meda.Config
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

func scanFiles(db *meda.DB) error {
	it := db.FilesIterator(context.Background(), meda.FilesIteratorConfig{
		ChunkSize: 100000,
		BatchSize: 10000,
	})

	var fileCount, lastLogFileCount, fileTotalSize uint64

	for it.Next() {
		fileTotalSize += it.Element().FileSize
		fileCount += 1

		if lastLogFileCount+1000000 <= fileCount {
			log.Printf("Read %d files so far...", fileCount)
			lastLogFileCount = fileCount
		}
	}
	if it.Error() != nil {
		return it.Error()
	}

	log.Printf("Completed reading")
	log.Printf("Read %d files with total size %d", fileCount, fileTotalSize)

	return nil
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

	db, err := meda.Open(meda.DefaultConfig.Clone().Merge(&config.DB))
	if err != nil {
		panic(err)
	}

	err = scanFiles(db)
	if err != nil {
		panic(err)
	}
}
