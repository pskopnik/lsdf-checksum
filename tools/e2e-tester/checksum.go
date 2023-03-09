package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/stretchr/objx"
)

type checksumRunSyncMode int

const (
	CRSM_FULL checksumRunSyncMode = iota
	CRSM_INCREMENTAL
)

func mustChecksumRunSyncModeToString(val checksumRunSyncMode) string {
	switch val {
	case CRSM_FULL:
		return "full"
	case CRSM_INCREMENTAL:
		return "incremental"
	default:
		panic(fmt.Sprintf("%v is not a known checksumRunMode", val))
	}
}

type checksumConfig struct {
	BaseConfigPath    string
	FileSystemName    string
	FileSystemSubpath string
	CoverDir          string
}

func (c *checksumConfig) DeriveFinalConfig() (string, error) {
	return deriveConfig(c.BaseConfigPath, func(m objx.Map) error {
		m.Set("master.filesystemname", c.FileSystemName)
		m.Set("master.filesystemsubpath", c.FileSystemSubpath)
		return nil
	})
}

type checksumRunConfig struct {
	ChecksumConfig checksumConfig
	RunMode        checksumRunSyncMode
	LogPath        string
	RaceLogPath    string
}

func runChecksumRun(binPath string, config checksumRunConfig) error {
	configPath, err := config.ChecksumConfig.DeriveFinalConfig()
	if err != nil {
		return fmt.Errorf("runChecksumRun: %w", err)
	}
	defer os.Remove(configPath)

	runMode := mustChecksumRunSyncModeToString(config.RunMode)

	cmd := exec.Command(binPath, "run", "--config", configPath, "--log-format", "logfmt", "--log-level", "debug", "--mode", runMode)
	if cmd.Err != nil {
		return fmt.Errorf("runChecksumRun: prepare command execution: %w", err)
	}

	if config.ChecksumConfig.CoverDir != "" {
		cmd.Env = append(cmd.Environ(), "GOCOVERDIR="+config.ChecksumConfig.CoverDir)
	}
	if config.RaceLogPath != "" {
		cmd.Env = append(cmd.Environ(), "GORACE=log_path="+config.RaceLogPath)
	}

	var logFile *os.File
	if config.LogPath != "" {
		logFile, err = os.Create(config.LogPath)
		if err != nil {
			return fmt.Errorf("runChecksumRun: open log file at %s: %w", config.LogPath, err)
		}
		defer logFile.Close()
	} else {
		logFile, err = os.CreateTemp("", "*_checksum_run.log")
		if err != nil {
			return fmt.Errorf("runChecksumRun: create temp log file: %w", err)
		}
		tempFileName := logFile.Name()
		defer os.Remove(tempFileName)
		defer logFile.Close()
	}
	cmd.Stdout = logFile

	stderrBuf := &bytes.Buffer{}
	cmd.Stderr = stderrBuf

	err = cmd.Run()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			if exitErr.ExitCode() == 66 && config.RaceLogPath != "" {
				// Simply ignore for now
			} else {
				lastLogLines, _ := readLastLines(logFile, 20)
				return fmt.Errorf("runChecksumRun: process exit: %w. "+
					"Last lines of log output:\n%s\n"+
					"Process stderr:\n%s", err, lastLogLines, stderrBuf.String(),
				)
			}
		} else {
			return fmt.Errorf("runChecksumRun: wait for process: %w", err)
		}
	}

	return nil
}

func readLastLines(r io.Reader, n int) (string, error) {
	lastLinesC := make(chan string, n)

	buffedR := bufio.NewReader(r)
	var readErr error
	for {
		var line string
		line, readErr = buffedR.ReadString('\n')
		if readErr == io.EOF {
			break
		} else if readErr != nil {
			break
		}

		if len(lastLinesC) == cap(lastLinesC) {
			<-lastLinesC
		}
		lastLinesC <- line
	}

	builder := strings.Builder{}
	for line := range lastLinesC {
		// Always returns nil error
		_, _ = builder.WriteString(line)
	}

	return builder.String(), nil
}

type checksumWarningsConfig struct {
	ChecksumConfig checksumConfig
	OnlyLastRun    bool
}

type checksumWarning struct {
	Path       string `json:"path"`
	Discovered uint64 `json:"discovered"`
	LastRead   uint64 `json:"last_read"`
}

func runChecksumWarnings(binPath string, config checksumWarningsConfig) ([]checksumWarning, error) {
	configPath, err := config.ChecksumConfig.DeriveFinalConfig()
	if err != nil {
		return nil, fmt.Errorf("runChecksumWarnings: %w", err)
	}
	defer os.Remove(configPath)

	args := []string{"warnings", "--config", configPath, "--format", "json"}
	if config.OnlyLastRun {
		args = append(args, "--only-last")
	}

	cmd := exec.Command(binPath, args...)
	if cmd.Err != nil {
		return nil, fmt.Errorf("runChecksumWarnings: prepare command execution: %w", err)
	}

	if config.ChecksumConfig.CoverDir != "" {
		cmd.Env = append(cmd.Environ(), "GOCOVERDIR="+config.ChecksumConfig.CoverDir)
	}

	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("runChecksumWarnings: process exit: %w. Process stderr:\n%s", err, string(exitErr.Stderr))
		} else {
			return nil, fmt.Errorf("runChecksumWarnings: wait for process: %w", err)
		}
	}

	var warnings []checksumWarning

	err = json.Unmarshal(out, &warnings)
	if err != nil {
		return nil, fmt.Errorf("runChecksumWarnings: decode warnings: %w", err)
	}

	return warnings, nil
}

type checksumRunsConfig struct {
	ChecksumConfig checksumConfig
	OnlyLastRun    bool
}

type checksumRun struct {
	ID           uint64          `json:"id"`
	SnapshotName *string         `json:"snapshot_name,omitempty"`
	SnapshotID   *uint64         `json:"snapshot_id,omitempty"`
	SyncMode     jsonRunSyncMode `json:"sync_mode"`
	State        string          `json:"state"`
}

type jsonRunSyncMode checksumRunSyncMode

func (j *jsonRunSyncMode) UnmarshalJSON(in []byte) error {
	var s string
	err := json.Unmarshal(in, &s)
	if err != nil {
		return err
	}

	switch s {
	case "full":
		*j = jsonRunSyncMode(CRSM_FULL)
	case "incremental":
		*j = jsonRunSyncMode(CRSM_INCREMENTAL)
	default:
		return fmt.Errorf("Unknown run sync mode '%s'", s)
	}

	return nil
}

func runChecksumRuns(binPath string, config checksumRunsConfig) ([]checksumRun, error) {
	configPath, err := config.ChecksumConfig.DeriveFinalConfig()
	if err != nil {
		return nil, fmt.Errorf("runChecksumRuns: %w", err)
	}
	defer os.Remove(configPath)

	args := []string{"runs", "--config", configPath, "--format", "json"}
	if config.OnlyLastRun {
		args = append(args, "--only-last")
	}

	cmd := exec.Command(binPath, args...)
	if cmd.Err != nil {
		return nil, fmt.Errorf("runChecksumRuns: prepare command execution: %w", err)
	}

	if config.ChecksumConfig.CoverDir != "" {
		cmd.Env = append(cmd.Environ(), "GOCOVERDIR="+config.ChecksumConfig.CoverDir)
	}

	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("runChecksumRuns: process exit: %w. Process stderr:\n%s", err, string(exitErr.Stderr))
		} else {
			return nil, fmt.Errorf("runChecksumRuns: wait for process: %w", err)
		}
	}

	var runs []checksumRun

	err = json.Unmarshal(out, &runs)
	if err != nil {
		return nil, fmt.Errorf("runChecksumRuns: decode warnings: %w", err)
	}

	return runs, nil
}
