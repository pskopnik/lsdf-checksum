package main

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/stretchr/objx"
)

type gentreeConfig struct {
	BaseConfigPath string
	RootDir        string
}

func runGentree(binPath string, config gentreeConfig) error {
	confPath, err := deriveConfig(config.BaseConfigPath, func(m objx.Map) error {
		m.Set("root_dir", config.RootDir)
		return nil
	})
	if err != nil {
		return fmt.Errorf("runGentree: %w", err)
	}
	defer os.Remove(confPath)

	cmd := exec.Command(binPath, confPath)
	if cmd.Err != nil {
		return fmt.Errorf("runGentree: prepare command execution: %w", err)
	}

	out, err := cmd.CombinedOutput()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			return fmt.Errorf("runGentree: gentree process exit: %w. Process output:\n%s", err, string(out))
		} else {
			return fmt.Errorf("runGentree: wait for gentree process: %w", err)
		}
	}

	return nil
}
