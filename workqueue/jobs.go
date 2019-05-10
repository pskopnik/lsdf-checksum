package workqueue

import (
	"encoding/base64"
	"errors"
)

// Error variables related to workqueue jobs definitions and encoding.
var (
	ErrTailingBytes        = errors.New("encoding format error: Unexpected tailing bytes")
	ErrPackKeyNotFound     = errors.New("pack key not found in Job's Args")
	ErrPackValueCastFailed = errors.New("failed to cast value of pack key in Job's Args")
)

const (
	CalculateChecksumJobName string = "CalculateChecksum"

	writeBackJobNameBase string = "WriteBack"

	WorkPackJobArgsKey      string = "pack"
	WriteBackPackJobArgsKey string = "pack"
)

func WriteBackJobName(fileSystemName, snapshotName string) string {
	return writeBackJobNameBase + "-" + fileSystemName + "-" + snapshotName
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
	msgpBuf, err := w.MarshalMsg(nil)
	if err != nil {
		return err
	}

	base64BufLen := base64.RawStdEncoding.EncodedLen(len(msgpBuf))
	base64Buf := make([]byte, base64BufLen)

	base64.RawStdEncoding.Encode(base64Buf, msgpBuf)

	jobArgs[WorkPackJobArgsKey] = string(base64Buf)

	return nil
}

func (w *WorkPack) FromJobArgs(jobArgs map[string]interface{}) error {
	packJobArgIntf, ok := jobArgs[WorkPackJobArgsKey]
	if !ok {
		return ErrPackKeyNotFound
	}
	packJobArg, ok := packJobArgIntf.(string)
	if !ok {
		return ErrPackValueCastFailed
	}

	base64Buf := []byte(packJobArg)
	msgpBufLen := base64.RawStdEncoding.DecodedLen(len(base64Buf))
	msgpBuf := make([]byte, msgpBufLen)

	_, err := base64.RawStdEncoding.Decode(msgpBuf, base64Buf)
	if err != nil {
		return err
	}

	msgpBuf, err = w.UnmarshalMsg(msgpBuf)
	if err != nil {
		return err
	}
	if len(msgpBuf) != 0 {
		return ErrTailingBytes
	}

	return nil
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
	msgpBuf, err := w.MarshalMsg(nil)
	if err != nil {
		return err
	}

	base64BufLen := base64.RawStdEncoding.EncodedLen(len(msgpBuf))
	base64Buf := make([]byte, base64BufLen)

	base64.RawStdEncoding.Encode(base64Buf, msgpBuf)

	jobArgs[WriteBackPackJobArgsKey] = string(base64Buf)

	return nil
}

func (w *WriteBackPack) FromJobArgs(jobArgs map[string]interface{}) error {
	packJobArgIntf, ok := jobArgs[WriteBackPackJobArgsKey]
	if !ok {
		return ErrPackKeyNotFound
	}
	packJobArg, ok := packJobArgIntf.(string)
	if !ok {
		return ErrPackValueCastFailed
	}

	base64Buf := []byte(packJobArg)
	msgpBufLen := base64.RawStdEncoding.DecodedLen(len(base64Buf))
	msgpBuf := make([]byte, msgpBufLen)

	_, err := base64.RawStdEncoding.Decode(msgpBuf, base64Buf)
	if err != nil {
		return err
	}

	msgpBuf, err = w.UnmarshalMsg(msgpBuf)
	if err != nil {
		return err
	}
	if len(msgpBuf) != 0 {
		return ErrTailingBytes
	}

	return nil
}
