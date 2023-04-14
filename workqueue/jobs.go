package workqueue

import (
	"encoding/base64"
	"errors"

	"github.com/tinylib/msgp/msgp"
)

// Error variables related to workqueue jobs definitions and encoding.
var (
	ErrTrailingBytes          = errors.New("encoding format error: Unexpected trailing bytes")
	ErrPayloadKeyNotFound     = errors.New("payload key not found in Job's Args")
	ErrPayloadValueCastFailed = errors.New("failed to cast value of payload key in Job's Args")
)

const (
	CalculateChecksumJobName string = "CalculateChecksum"

	writeBackJobNameBase string = "WriteBack"

	PayloadJobArgsKey string = "pl"
)

func WriteBackJobName(fileSystemName, snapshotName string) string {
	return writeBackJobNameBase + "-" + fileSystemName + "-" + snapshotName
}

type JobPayload interface {
	ToJobArgs(jobArgs map[string]interface{}) error
	FromJobArgs(jobArgs map[string]interface{}) error
}

//go:generate msgp

type WorkPackFile struct {
	ID   uint64
	Path string
}

type WorkPack struct {
	FileSystemName string
	SnapshotName   string
	Files          []WorkPackFile
}

func (w *WorkPack) ToJobArgs(jobArgs map[string]interface{}) error {
	return marshalPayloadToJobArgs(jobArgs, w)
}

func (w *WorkPack) FromJobArgs(jobArgs map[string]interface{}) error {
	return unmarshalPayloadFromJobArgs(jobArgs, w)
}

type WriteBackPackFile struct {
	ID       uint64
	Checksum []byte
}

func (w *WriteBackPackFile) DeepCopyFrom(other *WriteBackPackFile) {
	w.ID = other.ID
	w.Checksum = append(w.Checksum[:0], other.Checksum...)
}

type WriteBackPack struct {
	Files []WriteBackPackFile
}

func (w *WriteBackPack) ToJobArgs(jobArgs map[string]interface{}) error {
	return marshalPayloadToJobArgs(jobArgs, w)
}

func (w *WriteBackPack) FromJobArgs(jobArgs map[string]interface{}) error {
	return unmarshalPayloadFromJobArgs(jobArgs, w)
}

func cleanJobArgs(jobArgs map[string]interface{}) {
	for key := range jobArgs {
		if key != PayloadJobArgsKey {
			delete(jobArgs, key)
		}
	}
}

func marshalPayloadToJobArgs[T msgp.Marshaler](jobArgs map[string]interface{}, payload T) error {
	msgpBuf, err := payload.MarshalMsg(nil)
	if err != nil {
		return err
	}

	base64BufLen := base64.RawStdEncoding.EncodedLen(len(msgpBuf))
	base64Buf := make([]byte, base64BufLen)

	base64.RawStdEncoding.Encode(base64Buf, msgpBuf)

	cleanJobArgs(jobArgs)
	jobArgs[PayloadJobArgsKey] = base64Buf

	return nil
}

func unmarshalPayloadFromJobArgs[T msgp.Unmarshaler](jobArgs map[string]interface{}, payload T) error {
	packJobArgIntf, ok := jobArgs[PayloadJobArgsKey]
	if !ok {
		return ErrPayloadKeyNotFound
	}
	packJobArg, ok := packJobArgIntf.(string)
	if !ok {
		return ErrPayloadValueCastFailed
	}

	base64Buf := []byte(packJobArg)
	msgpBufLen := base64.RawStdEncoding.DecodedLen(len(base64Buf))
	msgpBuf := make([]byte, msgpBufLen)

	_, err := base64.RawStdEncoding.Decode(msgpBuf, base64Buf)
	if err != nil {
		return err
	}

	msgpBuf, err = payload.UnmarshalMsg(msgpBuf)
	if err != nil {
		return err
	}
	if len(msgpBuf) != 0 {
		return ErrTrailingBytes
	}

	return nil
}
