package main

import (
	"fmt"
	"os"

	"github.com/stretchr/objx"
	"gopkg.in/yaml.v2"
)

func loadConfigFileIntoMap(configPath string) (objx.Map, error) {
	f, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("loadConfigFileIntoMap: open config file: %w", err)
	}
	defer f.Close()

	config := make(map[string]interface{})
	dec := yaml.NewDecoder(f)
	err = dec.Decode(&config)
	if err != nil {
		return nil, fmt.Errorf("loadConfigFileIntoMap: decode config: %w", err)
	}
	err = f.Close()
	if err != nil {
		return nil, fmt.Errorf("loadConfigFileIntoMap: close config file: %w", err)
	}

	m := objx.New(config)

	// Trigger clean up logic
	_, _ = m.JSON()

	return m, nil
}

func deriveConfig(baseConfigPath string, f func(objx.Map) error) (string, error) {
	m, err := loadConfigFileIntoMap(baseConfigPath)
	if err != nil {
		return "", fmt.Errorf("deriveConfig: %w", err)
	}

	err = f(m)
	if err != nil {
		return "", fmt.Errorf("deriveConfig: delegate to mutate func: %w", err)
	}

	// Trigger clean up logic
	_, _ = m.JSON()

	finalConfigF, err := os.CreateTemp("", "*_derived_config.yaml")
	if err != nil {
		return "", fmt.Errorf("deriveConfig: create temp file: %w", err)
	}
	confPath := finalConfigF.Name()
	defer finalConfigF.Close()

	finalConfigEnc := yaml.NewEncoder(finalConfigF)
	err = finalConfigEnc.Encode(m)
	if err != nil {
		_ = os.Remove(confPath)
		return "", fmt.Errorf("deriveConfig: encode final config: %w", err)
	}
	err = finalConfigF.Close()
	if err != nil {
		_ = os.Remove(confPath)
		return "", fmt.Errorf("deriveConfig: close temp file: %w", err)
	}

	return confPath, nil
}
