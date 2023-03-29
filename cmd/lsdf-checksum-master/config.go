package main

import (
	"os"

	"github.com/apex/log"
	"gopkg.in/yaml.v3"

	"git.scc.kit.edu/sdm/lsdf-checksum/master"
	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

type Config struct {
	Master master.Config
	DB     meda.Config
}

func prepareConfig(file *os.File, leaveOpen bool, logger log.Interface) (*Config, error) {
	config, err := readConfig(file)
	if err != nil {
		if !leaveOpen {
			_ = file.Close()
		}

		logger.WithError(err).WithFields(log.Fields{
			"name": file.Name(),
		}).Error("Encountered error while reading config file")

		return nil, err
	}

	if !leaveOpen {
		err = file.Close()
		if err != nil {
			logger.WithError(err).WithFields(log.Fields{
				"name": file.Name(),
			}).Error("Encountered error while closing config file")

			return nil, err
		}
	}

	return config, nil
}

func readConfig(file *os.File) (*Config, error) {
	decoder := yaml.NewDecoder(file)

	yamlConfig := &Config{}

	err := decoder.Decode(yamlConfig)
	if err != nil {
		return nil, err
	}

	return yamlConfig, nil
}
