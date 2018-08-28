package main

import (
	"bufio"
	"bytes"
	"io"
)

var _ io.Reader = &LineFilter{}

type LineFilter struct {
	// MaxLineLength is the maximum length of a line (number of bytes)
	// (including the newline character '\n') which is "allowed" by the
	// LineFilter.
	// All lines longer than this will be omitted.
	// This will initially be set to bufio.MaxScanTokenSize.
	MaxLineLength int

	r         io.Reader
	inBuf     *bufio.Reader
	line      []byte
	outReader bytes.Reader
}

func NewLineFilter(r io.Reader) *LineFilter {
	return &LineFilter{
		MaxLineLength: bufio.MaxScanTokenSize,
		r:             r,
		inBuf:         bufio.NewReader(r),
		line:          make([]byte, 0, 1024),
	}
}

func (l *LineFilter) Read(p []byte) (int, error) {
	if l.outReader.Len() == 0 {
		l.readNextLine()
	}
	return l.outReader.Read(p)
}

func (l *LineFilter) readNextLine() error {
	l.line = l.line[:0]

	for {
		chunk, isPrefix, err := l.inBuf.ReadLine()
		if err != nil {
			return err
		}
		if len(l.line)+len(chunk)+1 > l.MaxLineLength {
			l.line = l.line[:0]

			err = l.seekToEndOfLine()
			if err != nil {
				return err
			}

			continue
		}

		l.line = append(l.line, chunk...)

		if !isPrefix {
			break
		}
	}

	l.line = append(l.line, '\n')

	l.outReader.Reset(l.line)
	return nil
}

func (l *LineFilter) seekToEndOfLine() error {
	for {
		_, isPrefix, err := l.inBuf.ReadLine()
		if err != nil {
			return err
		}

		if !isPrefix {
			break
		}
	}

	return nil
}
