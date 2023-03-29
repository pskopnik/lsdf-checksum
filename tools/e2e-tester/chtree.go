package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"

	"gopkg.in/yaml.v3"
)

type changedFile struct {
	Path          string `json:"path"`
	CorruptedOnly bool   `json:"corrupted_only"`
}

type chtreeConfig struct {
	RootDir  string                `yaml:"root_dir"`
	Seed     uint64                `yaml:"seed,omitempty"`
	Changers []chtreeConfigChanger `yaml:"changers"`
}

type chtreeConfigChanger struct {
	Append     *chtreeConfigAppend   `yaml:"append,omitempty"`
	Replace    *chtreeConfigReplace  `yaml:"replace,omitempty"`
	FlipByte   *chtreeConfigFlipByte `yaml:"flipbyte,omitempty"`
	Likelihood float64               `yaml:"likelihood"`
	ID         string                `yaml:"id,omitempty"`
	Corrupt    bool                  `yaml:"corrupt,omitempty"`
}

type chtreeConfigAppend struct {
	Size int64 `yaml:"size"`
}

type chtreeConfigReplace struct {
	Size int64 `yaml:"size"`
}

type chtreeConfigFlipByte struct{}

func runChtree(binPath string, config chtreeConfig) ([]changedFile, error) {
	confF, err := os.CreateTemp("", "*")
	if err != nil {
		return nil, fmt.Errorf("runChtree: create temp file: %w", err)
	}
	confPath := confF.Name()
	defer confF.Close()
	defer os.Remove(confPath)

	confEnc := yaml.NewEncoder(confF)
	confEnc.Encode(config)
	if err != nil {
		return nil, fmt.Errorf("runChtree: encode config: %w", err)
	}
	err = confF.Close()
	if err != nil {
		return nil, fmt.Errorf("runChtree: close temp file: %w", err)
	}

	cmd := exec.Command(binPath, confPath)
	if cmd.Err != nil {
		return nil, fmt.Errorf("runChtree: prepare command execution: %w", err)
	}

	r, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("runChtree: init stdout pipe: %w", err)
	}

	stderrBuf := &bytes.Buffer{}
	cmd.Stderr = stderrBuf

	err = cmd.Start()
	if err != nil {
		return nil, fmt.Errorf("runChtree: start command: %w", err)
	}

	dec := json.NewDecoder(r)
	var changes []changedFile
	var decErr error

	for {
		changes = append(changes, changedFile{})
		decErr = dec.Decode(&changes[len(changes)-1])
		if decErr == io.EOF {
			changes = changes[:len(changes)-1]
			decErr = nil
			break
		} else if decErr != nil {
			break
		}
	}

	err = cmd.Wait()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("runChtree: chtree process exit: %w. Process stderr:\n%s", err, stderrBuf.String())
		} else {
			return nil, fmt.Errorf("runChtree: wait for chtree process: %w", err)
		}
	}
	if decErr != nil {
		return nil, fmt.Errorf("runChtree: output decoding (chtree process exited successfully): %w", decErr)
	}

	return changes, nil
}

func buildChtreeConfig(rootDir string, changeLikelihood, corruptLikelihood float64) (config chtreeConfig) {
	config.RootDir = rootDir

	if changeLikelihood > 0 {
		// Overall change likelihood is deconstructed into per-changer likelihood
		// by solving the following:
		// (1-perChangerLikelihood)^2 = 1 - changeLikelihood
		perChangerLikelihood := 1 - math.Sqrt(1-changeLikelihood)

		config.Changers = append(
			config.Changers,
			chtreeConfigChanger{
				Append: &chtreeConfigAppend{
					Size: 100,
				},
				Likelihood: perChangerLikelihood,
			},
			chtreeConfigChanger{
				Replace: &chtreeConfigReplace{
					Size: 100,
				},
				Likelihood: perChangerLikelihood,
			},
		)
	}

	if corruptLikelihood > 0 {
		config.Changers = append(config.Changers, chtreeConfigChanger{
			FlipByte:   &chtreeConfigFlipByte{},
			Likelihood: corruptLikelihood,
			Corrupt:    true,
		})
	}

	return
}
