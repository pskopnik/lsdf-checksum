package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/olekukonko/tablewriter"

	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

var (
	ErrChecksumWarningExists = errors.New("at least one checksum warnings exists")
)

var (
	warnings           = app.Command("warnings", "Print a list of checksum warnings.")
	warningsConfigFile = warnings.Flag("config", "Path to the configuration file.").Short('c').PlaceHolder("config.yaml").Required().File()
	warningsExit       = warnings.Flag("exit", "Exit with code 2 if the command returns any checksums.").Default("false").Bool()
	warningsRun        = warnings.Flag("run", "Only print warnings emitted by the specified run.").PlaceHolder("ID").Uint64()
	warningsOnlyLast   = warnings.Flag("only-last", "Only print warnings emitted by the very last run.").Default("false").Bool()
	warningsOnlyLastN  = warnings.Flag("only-last-n", "Only print warnings emitted by the last n runs.").PlaceHolder("N").Short('n').Uint64()
	warningsClear      = warnings.Flag("clear", "Clear the printed warnings. The warnings are permanently deleted from the database.").Default("false").Bool()
	warningsFormat     = warnings.Flag("format", "Format of the output. text prints an ASCII table (the default). json prints a JSON list.").Short('f').Default("text").Enum("text", "json")
)

func performWarnings() error {
	ctx := context.Background()
	logger := prepareNoOpLogger()

	config, err := prepareConfig(*warningsConfigFile, false, logger)
	if err != nil {
		return err
	}
	db, err := openDB(ctx, &config.DB)
	if err != nil {
		return err
	}

	warnings, err := warningsFetchChecksumWarnings(ctx, db)
	if err != nil {
		return err
	}

	switch *warningsFormat {
	case "text":
		writeChecksumWarningsAsText(os.Stdout, warnings)
	case "json":
		writeChecksumWarningsAsJSON(os.Stdout, warnings)
	default:
		panic(fmt.Sprintf("warnings: unsupported format '%s'", *runsFormat))
	}

	if *warningsClear {
		_, err := db.ChecksumWarningsDeleteChecksumWarnings(ctx, nil, warnings)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "Encountered error while deleting checksum warnings:", err)
			return err
		}
	}

	if *warningsExit {
		if len(warnings) > 0 {
			return &MainError{
				error:    ErrChecksumWarningExists,
				ExitCode: 2,
			}
		}
	}

	return nil
}

func warningsFetchChecksumWarnings(ctx context.Context, db *meda.DB) (warnings []meda.ChecksumWarning, err error) {
	if *warningsRun != 0 {
		warnings, err = db.ChecksumWarningsFetchFromRunByID(ctx, nil, *warningsRun)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "Encountered error while fetching checksum warnings from run by id from database:", err)
			return
		}
	} else if *warningsOnlyLastN != 0 || *warningsOnlyLast {
		var n uint64
		if *warningsOnlyLastN != 0 {
			n = *warningsOnlyLastN
		} else if *warningsOnlyLast {
			n = 1
		}

		warnings, err = db.ChecksumWarningsFetchFromLastNRuns(ctx, nil, n)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "Encountered error while fetching checksum warnings from last n runs from database:", err)
			return
		}
	} else {
		warnings, err = db.ChecksumWarningsFetchAll(ctx, nil)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "Encountered error while fetching all checksum warnings from database:", err)
			return
		}
	}

	return
}

func writeChecksumWarningsAsText(w io.Writer, warnings []meda.ChecksumWarning) error {
	table := tablewriter.NewWriter(w)

	table.SetHeader([]string{
		"ID",
		"File ID",
		"Path",
		"Modification Time",
		"File Size",
		"Expected Checksum",
		"Actual Checksum",
		"Discovered",
		"Last Read",
		"Created",
	})

	for i := range warnings {
		warning := &warnings[i]

		id := strconv.FormatUint(warning.ID, 10)
		fileID := strconv.FormatUint(warning.FileID, 10)
		path := warning.Path
		modificationTime := time.Time(warning.ModificationTime).String()
		fileSize := strconv.FormatUint(warning.FileSize, 10)
		expectedChecksum := hex.EncodeToString(warning.ExpectedChecksum)
		actualChecksum := hex.EncodeToString(warning.ActualChecksum)
		discovered := strconv.FormatUint(warning.Discovered, 10)
		lastRead := strconv.FormatUint(warning.LastRead, 10)
		created := time.Time(warning.Created).String()

		table.Append([]string{
			id,
			fileID,
			path,
			modificationTime,
			fileSize,
			expectedChecksum,
			actualChecksum,
			discovered,
			lastRead,
			created,
		})
	}

	table.Render()

	return nil
}

type jsonChecksumWarning struct {
	ID               uint64       `json:"id"`
	FileID           uint64       `json:"file_id"`
	Path             string       `json:"path"`
	ModificationTime time.Time    `json:"modification_time"`
	FileSize         uint64       `json:"file_size"`
	ExpectedChecksum jsonHexBytes `json:"expected_checksum"`
	ActualChecksum   jsonHexBytes `json:"actual_checksum"`
	Discovered       uint64       `json:"discovered"`
	LastRead         uint64       `json:"last_read"`
	Created          time.Time    `json:"created"`
}

type jsonHexBytes []byte

func (j jsonHexBytes) MarshalJSON() ([]byte, error) {
	return json.Marshal(hex.EncodeToString([]byte(j)))
}

func writeChecksumWarningsAsJSON(w io.Writer, warnings []meda.ChecksumWarning) error {
	jsonWarnings := make([]jsonChecksumWarning, len(warnings))

	for i := range warnings {
		warning := &warnings[i]

		jsonWarning := jsonChecksumWarning{
			ID:               warning.ID,
			FileID:           warning.FileID,
			Path:             warning.Path,
			ModificationTime: time.Time(warning.ModificationTime),
			FileSize:         warning.FileSize,
			ExpectedChecksum: jsonHexBytes(warning.ExpectedChecksum),
			ActualChecksum:   jsonHexBytes(warning.ActualChecksum),
			Discovered:       warning.Discovered,
			LastRead:         warning.LastRead,
			Created:          time.Time(warning.Created),
		}

		jsonWarnings[i] = jsonWarning
	}

	enc := json.NewEncoder(w)

	return enc.Encode(jsonWarnings)
}
