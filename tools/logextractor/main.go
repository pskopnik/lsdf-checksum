package main

import (
	"encoding/csv"
	"os"
	"strconv"
	"time"
)

const csvFields = 15

var csvHeaders = [...]string{
	"time",
	"timestamp",
	"previous_queue_length",
	"queue_length",
	"exhausted",
	"enqueued",
	"worker_num",
	"consumption",
	"deviation",
	"consumption_alpha",
	"consumption_ewma",
	"deviation_alpha",
	"deviation_ewma",
	"phase",
	"threshold",
}

func main() {
	logFilePath := os.Args[1]
	csvFilePath := os.Args[2]

	logFile, err := os.Open(logFilePath)
	if err != nil {
		panic(err)
	}
	defer logFile.Close()

	csvFile, err := os.Create(csvFilePath)
	if err != nil {
		panic(err)
	}
	defer csvFile.Close()

	scanner := NewScanner(NewLineFilter(logFile))

	csvWriter := csv.NewWriter(csvFile)
	err = csvWriter.Write(csvHeaders[:])
	if err != nil {
		panic(err)
	}

	var snapshot *SchedulingSnapshot

	for scanner.Next() {
		snapshot = scanner.Snapshot()
		row := snapshotToCSVRow(snapshot)
		err = csvWriter.Write(row[:])
		if err != nil {
			panic(err)
		}
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}

	csvWriter.Flush()
}

func boolToString(b bool) string {
	switch b {
	case true:
		return "true"
	default:
		return "false"
	}
}

func snapshotToCSVRow(snapshot *SchedulingSnapshot) [csvFields]string {
	return [...]string{
		strconv.FormatInt(snapshot.Time.Unix(), 10),
		snapshot.Time.Format(time.RFC3339),
		strconv.FormatUint(uint64(snapshot.PreviousQueueLength), 10),
		strconv.FormatInt(snapshot.QueueLength, 10),
		boolToString(snapshot.Exhausted),
		strconv.FormatUint(uint64(snapshot.Enqueued), 10),
		strconv.FormatInt(int64(snapshot.WorkerNum), 10),
		strconv.FormatUint(uint64(snapshot.Consumption), 10),
		strconv.FormatFloat(snapshot.Deviation, 'G', -1, 64),
		strconv.FormatFloat(snapshot.ConsumptionAlpha, 'G', -1, 64),
		strconv.FormatFloat(snapshot.ConsumptionEWMA, 'G', -1, 64),
		strconv.FormatFloat(snapshot.DeviationAlpha, 'G', -1, 64),
		strconv.FormatFloat(snapshot.DeviationEWMA, 'G', -1, 64),
		snapshot.Phase.String(),
		strconv.FormatUint(uint64(snapshot.Threshold), 10),
	}
}
