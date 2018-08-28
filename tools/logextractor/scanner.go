package main

import (
	"bytes"
	"errors"
	"io"
	"strconv"
	"time"

	"github.com/go-logfmt/logfmt"
)

type SchedulerPhase int8

const (
	SPUninitialised SchedulerPhase = iota
	SPStartUp
	SPMaintaining
	SPUnknown
)

func (s SchedulerPhase) String() string {
	switch s {
	case SPUninitialised:
		return "uninitialised"
	case SPStartUp:
		return "startup"
	case SPMaintaining:
		return "maintaining"
	case SPUnknown:
		return "unknown"
	default:
		return "unknown"
	}
}

func SchedulerPhaseFromString(str string) SchedulerPhase {
	switch str {
	case "uninitialised":
		return SPUninitialised
	case "startup":
		return SPStartUp
	case "maintaining":
		return SPMaintaining
	case "unknown":
		return SPUnknown
	default:
		return SPUnknown
	}
}

var (
	errInvalidBoolValue = errors.New("invalid bool value")
)

func parseBool(str string) (value bool, err error) {
	switch str {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, nil
	}
}

type SchedulingSnapshot struct {
	Time time.Time

	PreviousQueueLength uint
	QueueLength         int64
	Exhausted           bool
	Enqueued            uint
	WorkerNum           int
	Consumption         uint
	Deviation           float64
	ConsumptionAlpha    float64
	ConsumptionEWMA     float64
	DeviationAlpha      float64
	DeviationEWMA       float64

	Phase     SchedulerPhase
	Threshold uint
}

func (s *SchedulingSnapshot) reset() {
	s.Time = time.Time{}

	s.PreviousQueueLength = 0
	s.QueueLength = 0
	s.Exhausted = false
	s.Enqueued = 0
	s.WorkerNum = 0
	s.Consumption = 0
	s.Deviation = 0.0
	s.ConsumptionAlpha = 0.0
	s.ConsumptionEWMA = 0.0
	s.DeviationAlpha = 0.0
	s.DeviationEWMA = 0.0

	s.Phase = SPUninitialised
	s.Threshold = 0
}

// observationFromEntry populates all observation attributes of the
// SchedulingSnapshot.
// If err != nil, the SchedulingSnapshot is left in an undefined state, some
// fields may be populated while others are not. Fields also may have invalid
// values.
func (s *SchedulingSnapshot) observationFromEntry(entry entryFields) (err error) {
	var tmpUint64 uint64
	var tmpInt64 int64

	for _, field := range entry {
		switch field.key {
		case "previous_queue_length":
			tmpUint64, err = strconv.ParseUint(field.value, 10, 64)
			if err != nil {
				return
			}
			s.PreviousQueueLength = uint(tmpUint64)
		case "queue_length":
			s.QueueLength, err = strconv.ParseInt(field.value, 10, 64)
			if err != nil {
				return
			}
		case "exhausted":
			s.Exhausted, err = parseBool(field.value)
			if err != nil {
				return
			}
		case "enqueued":
			tmpUint64, err = strconv.ParseUint(field.value, 10, 64)
			if err != nil {
				return
			}
			s.Enqueued = uint(tmpUint64)
		case "worker_num":
			tmpInt64, err = strconv.ParseInt(field.value, 10, 64)
			if err != nil {
				return
			}
			s.WorkerNum = int(tmpInt64)
		case "consumption":
			tmpUint64, err = strconv.ParseUint(field.value, 10, 64)
			if err != nil {
				return
			}
			s.Consumption = uint(tmpUint64)
		case "deviation":
			s.Deviation, err = strconv.ParseFloat(field.value, 64)
			if err != nil {
				return
			}
		case "consumption_alpha":
			s.ConsumptionAlpha, err = strconv.ParseFloat(field.value, 64)
			if err != nil {
				return
			}
		case "consumption_ewma":
			s.ConsumptionEWMA, err = strconv.ParseFloat(field.value, 64)
			if err != nil {
				return
			}
		case "deviation_alpha":
			s.DeviationAlpha, err = strconv.ParseFloat(field.value, 64)
			if err != nil {
				return
			}
		case "deviation_ewma":
			s.DeviationEWMA, err = strconv.ParseFloat(field.value, 64)
			if err != nil {
				return
			}
		}
	}

	return nil
}

// schedulingFromEntry populates all scheduling attributes of the
// SchedulingSnapshot.
// If err != nil, the SchedulingSnapshot is left in an undefined state, some
// fields may be populated while others are not. Fields also may have invalid
// values.
func (s *SchedulingSnapshot) schedulingFromEntry(entry entryFields) (err error) {
	var tmpUint64 uint64

	for _, field := range entry {
		switch field.key {
		case "phase":
			s.Phase = SchedulerPhaseFromString(field.value)
		case "threshold":
			tmpUint64, err = strconv.ParseUint(field.value, 10, 64)
			if err != nil {
				return
			}
			s.Threshold = uint(tmpUint64)
		}
	}

	return nil
}

type entryField struct {
	key   string
	value string
}

type entryFields []entryField

func (e entryFields) get(key string) (value string, ok bool) {
	for _, field := range e {
		if field.key == key {
			return field.value, true
		}
	}

	return "", false
}

type threeState int8

const (
	tsUnset threeState = iota
	tsFalse
	tsTrue
)

type scannerSearchState int8

const (
	sssUninitialised scannerSearchState = iota
	sssSearchingObserving
	sssSearchingScheduling
)

var (
	messageFieldName         = []byte("msg")
	observationRecordMessage = []byte("Probed queue and calculated derivatives")
	schedulingRecordMessage  = []byte("Calculated threshold")
	exhaustedRecordMessage   = []byte("Exhausted, doubled threshold")

	timeFieldName = "time"

	errNoMatchingRecord    = errors.New("not a matching record")
	errNoObservationRecord = errors.New("not an observation record")
	errNoSchedulingRecord  = errors.New("not an scheduling record")
	errTimeMissing         = errors.New("time field missing from record")
)

type Scanner struct {
	r        io.Reader
	dec      *logfmt.Decoder
	err      error
	entry    entryFields
	snapshot SchedulingSnapshot
}

func NewScanner(r io.Reader) *Scanner {
	return &Scanner{
		r:   r,
		dec: logfmt.NewDecoder(r),
	}
}

func (s *Scanner) Next() bool {
	var state scannerSearchState

	if s.err != nil {
		return false
	}

	s.snapshot.reset()
	state = sssSearchingObserving

	for s.dec.ScanRecord() {
		switch state {
		case sssSearchingObserving:
			err := s.processObservationRecord()
			if err == errNoObservationRecord {
				continue
			} else if err != nil {
				s.err = err
				return false
			}

			state = sssSearchingScheduling
		case sssSearchingScheduling:
			err := s.processSchedulingRecord()
			if err == errNoSchedulingRecord {
				continue
			} else if err != nil {
				s.err = err
				return false
			}

			return true
		}
	}
	if err := s.dec.Err(); err != nil {
		s.err = err
		return false
	}

	return false
}

func (s *Scanner) processObservationRecord() error {
	err := s.processRecord(observationRecordMessage)
	if err == errNoMatchingRecord {
		return errNoObservationRecord
	} else if err != nil {
		return err
	}

	strTime, ok := s.entry.get(timeFieldName)
	if !ok {
		return errTimeMissing
	}
	parsedTime, err := time.Parse(time.RFC3339, strTime)
	s.snapshot.Time = parsedTime

	s.snapshot.observationFromEntry(s.entry)

	return nil
}

func (s *Scanner) processSchedulingRecord() error {
	err := s.processRecord(schedulingRecordMessage)
	if err == errNoMatchingRecord {
		return errNoSchedulingRecord
	} else if err != nil {
		return err
	}

	s.snapshot.schedulingFromEntry(s.entry)

	return nil
}

func (s *Scanner) processRecord(message []byte) error {
	var skipRecord threeState
	s.entry = s.entry[:0]

	for s.dec.ScanKeyval() {
		if skipRecord == tsUnset && bytes.Equal(s.dec.Key(), messageFieldName) {
			if bytes.Equal(s.dec.Value(), message) {
				skipRecord = tsFalse
			} else {
				skipRecord = tsTrue
				break
			}
		}
		s.entry = append(s.entry, entryField{
			key:   string(s.dec.Key()),
			value: string(s.dec.Value()),
		})
	}
	if err := s.dec.Err(); err != nil {
		return err
	}
	if skipRecord == tsTrue || skipRecord == tsUnset {
		return errNoMatchingRecord
	}

	return nil
}

func (s *Scanner) Err() error {
	return s.err
}

func (s *Scanner) Snapshot() *SchedulingSnapshot {
	return &s.snapshot
}
