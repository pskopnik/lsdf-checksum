package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/olekukonko/tablewriter"

	"git.scc.kit.edu/sdm/lsdf-checksum/meda"
)

var (
	runs               = app.Command("runs", "Print a list of runs.")
	runsConfigFile     = runs.Flag("config", "Path to the configuration file.").Short('c').PlaceHolder("config.yaml").Required().File()
	runsRun            = runs.Flag("run", "Only print data of the specified run.").PlaceHolder("ID").Uint64()
	runsOnlyLast       = runs.Flag("only-last", "Only print data of the very last run.").Default("false").Bool()
	runsOnlyLastN      = runs.Flag("only-last-n", "Only print data of the last n runs.").Short('n').PlaceHolder("N").Uint64()
	runsOnlyIncomplete = runs.Flag("only-incomplete", "Only print data of incomplete runs.").Default("false").Bool()
	runsFormat         = runs.Flag("format", "Format of the output. text prints an ASCII table (the default). json prints JSON lines.").Short('f').Default("text").Enum("text", "json")
)

func performRuns() error {
	ctx := context.Background()
	logger := prepareNoOpLogger()

	config, err := prepareConfig(*runsConfigFile, false, logger)
	if err != nil {
		return err
	}
	db, err := openDB(ctx, &config.DB)
	if err != nil {
		return err
	}

	runs, err := runsFetchRuns(ctx, db)
	if err != nil {
		return err
	}

	switch *runsFormat {
	case "text":
		writeRunsAsText(os.Stdout, runs)
	case "json":
		writeRunsAsJSON(os.Stdout, runs)
	default:
		panic(fmt.Sprintf("runs: unsupported format '%s'", *runsFormat))
	}

	return nil
}

func runsFetchRuns(ctx context.Context, db *meda.DB) (runs []meda.Run, err error) {
	if *runsRun != 0 {
		var run meda.Run
		run, err = db.RunsQueryByID(ctx, nil, *runsRun)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "Encountered error while fetching run by id from database:", err)
			return
		}
		runs = []meda.Run{run}
	} else if *runsOnlyIncomplete {
		runs, err = db.RunsFetchIncomplete(ctx, nil)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "Encountered error while fetching incomplete runs from database:", err)
			return
		}
	} else if *runsOnlyLastN != 0 || *runsOnlyLast {
		var n uint64
		if *runsOnlyLastN != 0 {
			n = *runsOnlyLastN
		} else if *runsOnlyLast {
			n = 1
		}

		runs, err = db.RunsFetchLastN(ctx, nil, n)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "Encountered error while fetching last n runs from database:", err)
			return
		}
	} else {
		runs, err = db.RunsFetchAll(ctx, nil)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "Encountered error while fetching all runs from database:", err)
			return
		}
	}

	return
}

func writeRunsAsText(w io.Writer, runs []meda.Run) error {
	table := tablewriter.NewWriter(w)

	table.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
	table.SetCenterSeparator("|")

	table.SetHeader([]string{
		"ID",
		"Snapshot Name",
		"Snapshot ID",
		"Run At",
		"Sync Mode",
		"State",
	})

	for i := range runs {
		run := &runs[i]

		id := strconv.FormatUint(run.ID, 10)
		snapshotName := tablewriter.ConditionString(
			run.SnapshotName.Valid,
			run.SnapshotName.String,
			"",
		)
		snapshotID := tablewriter.ConditionString(
			run.SnapshotID.Valid,
			strconv.FormatUint(run.SnapshotID.Uint64, 10),
			"",
		)
		runAt := tablewriter.ConditionString(
			run.RunAt.Valid,
			run.RunAt.Time.String(),
			"",
		)
		syncMode := run.SyncMode.String()
		state := run.State.String()

		table.Append([]string{
			id,
			snapshotName,
			snapshotID,
			runAt,
			syncMode,
			state,
		})
	}

	table.Render()

	return nil
}

type jsonRun struct {
	ID           uint64          `json:"id"`
	SnapshotName *string         `json:"snapshot_name,omitempty"`
	SnapshotID   *uint64         `json:"snapshot_id,omitempty"`
	RunAt        *time.Time      `json:"run_at,omitempty"`
	SyncMode     jsonRunSyncMode `json:"sync_mode"`
	State        jsonRunState    `json:"state"`
}

type jsonRunSyncMode meda.RunSyncMode

func (j jsonRunSyncMode) MarshalJSON() ([]byte, error) {
	return json.Marshal(meda.RunSyncMode(j).String())
}

type jsonRunState meda.RunState

func (j jsonRunState) MarshalJSON() ([]byte, error) {
	return json.Marshal(meda.RunState(j).String())
}

func writeRunsAsJSON(w io.Writer, runs []meda.Run) error {
	enc := json.NewEncoder(w)

	for i := range runs {
		run := &runs[i]

		jsonRun := jsonRun{
			ID:       run.ID,
			SyncMode: jsonRunSyncMode(run.SyncMode),
			State:    jsonRunState(run.State),
		}

		if run.SnapshotName.Valid {
			jsonRun.SnapshotName = &run.SnapshotName.String
		}
		if run.SnapshotID.Valid {
			jsonRun.SnapshotID = &run.SnapshotID.Uint64
		}
		if run.RunAt.Valid {
			jsonRun.RunAt = &run.RunAt.Time
		}

		err := enc.Encode(jsonRun)
		if err != nil {
			return err
		}
	}

	return nil
}
