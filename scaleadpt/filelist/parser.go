package filelist

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/philhofer/fwd"
)

type FileData struct {
	Inode            uint64
	Generation       uint64
	SnapshotID       uint64
	FileSize         uint64
	ModificationTime time.Time
	Path             string
}

const (
	timeLayout = "2006-01-02 15:04:05"
)

var (
	ErrFormat            = errors.New("input does not conform to format")
	ErrMaxLengthExceeded = errors.New("maximum length exceeded")
)

type Parser struct {
	r *fwd.Reader
}

// NewParser constructs a new Parser reading from the passed in r.
// The Parser buffers internally, so there is generally no need for r
// buffering.
func NewParser(r io.Reader) *Parser {
	parser := &Parser{
		r: fwd.NewReaderSize(r, 4096),
	}

	return parser
}

// ParseLine parses a single line and writes the extracted data into fileData.
// When the end of the reader is reached, io.EOF is returned.
func (p *Parser) ParseLine(fileData *FileData) error {
	var err error

	// This is the only place, where an EOF is returned
	if _, err = p.r.Peek(1); err == io.EOF {
		return io.EOF
	}

	err = p.parsePreamble(fileData)
	if err != nil {
		return err
	}

	// Read space after preamble
	err = expectNextByteRead(p.r, ' ')
	if err != nil {
		return err
	}

	// Skip additional whitespace chars
	_, err = skipWhileCond(p.r, isAsciiSpace)
	if err != nil {
		return noEOF(err)
	}

	err = p.parseCustomAttributes(fileData)
	if err != nil {
		return err
	}

	err = p.expectFilenameSeparator()
	if err != nil {
		return err
	}

	err = p.parseFilename(fileData)
	if err != nil {
		return err
	}

	// Read newline, but also allow EOF without final newline
	err = expectNextByteRead(p.r, '\n')
	if err != nil && err != io.ErrUnexpectedEOF {
		return err
	}

	return nil
}

// parsePreamble parses the three numbers (inode, generation snapshot id) at
// the beginning of a line and writes the values to fileData.
// It consumes exactly until the end of the last number.
func (p *Parser) parsePreamble(fileData *FileData) error {
	var err error

	fileData.Inode, err = readNextUint64(p.r, ' ')
	if err != nil {
		return err
	}

	err = expectNextByteRead(p.r, ' ')
	if err != nil {
		return err
	}

	fileData.Generation, err = readNextUint64(p.r, ' ')
	if err != nil {
		return err
	}

	err = expectNextByteRead(p.r, ' ')
	if err != nil {
		return err
	}

	fileData.SnapshotID, err = readNextUint64(p.r, ' ')
	if err != nil {
		return err
	}

	return nil
}

// parseCustomAttributes parses the attributes added via SHOW() and writes the
// values to fileData.
// It consumes exactly the custom attribute section including the surrounding
// '|' chars.
func (p *Parser) parseCustomAttributes(fileData *FileData) error {
	var err error

	err = expectNextByteRead(p.r, '|')
	if err != nil {
		return err
	}

	fileData.FileSize, err = readNextUint64(p.r, '|')
	if err != nil {
		return err
	}

	err = expectNextByteRead(p.r, '|')
	if err != nil {
		return err
	}

	fileData.ModificationTime, err = readNextTime(p.r, '|', timeLayout)
	if err != nil {
		return err
	}

	err = expectNextByteRead(p.r, '|')
	if err != nil {
		return err
	}

	return nil
}

// parseFilename parses the file name at the end of the line and writes it to
// fileData after decoding the percent-encoding.
// It consumes exactly the file name and stops before the newline char but
// also accept EOF.
func (p *Parser) parseFilename(fileData *FileData) error {
	buf, err := peekUntilDelim(p.r, '\n', 16384)
	// EOF is allowed here
	if err != nil && err != io.EOF {
		return err
	}
	_, err = p.r.Skip(len(buf))
	if err != nil {
		return err
	}

	pathBytes, err := appendUnescapeUrl(make([]byte, 0, len(buf)), buf)
	if err != nil {
		return err
	}

	fileData.Path = string(pathBytes)

	return nil
}

// expectFilenameSeparator expects the separator " -- " between custom
// attributes and filename to be read.
// If the read chars don't match this, an error wrapped in ErrFormat is
// returned.
func (p *Parser) expectFilenameSeparator() error {
	buf, err := p.r.Next(4)
	if err != nil {
		return err
	}

	if !bytes.Equal(buf, []byte(" -- ")) {
		return fmt.Errorf("read '%x' instead of filename separator: %w", buf, ErrFormat)
	}

	return nil
}

// readNextUint64 reads the next uint64 from the reader, delimited either by
// delim or EOF.
// The delim is not consumed.
func readNextUint64(r *fwd.Reader, delim byte) (num uint64, err error) {
	var buf []byte

	buf, err = peekUntilDelim(r, delim, 32) // Arbitrary max which should fit all uints
	// EOF is allowed here
	if err != nil && err != io.EOF {
		return
	}

	num, err = strconv.ParseUint(string(buf), 10, 64)
	if err != nil {
		return
	}
	_, err = r.Skip(len(buf))
	if err != nil {
		// Should never err
		return
	}

	return
}

// readNextTime reads the next uint64 from the reader, delimited either by
// delim or EOF.
// The delim is not consumed.
func readNextTime(r *fwd.Reader, delim byte, layout string) (t time.Time, err error) {
	var buf []byte

	buf, err = peekUntilDelim(r, delim, 64) // Arbitrary max which should fit all date & time
	// EOF is allowed here
	if err != nil && err != io.EOF {
		return
	}

	t, err = time.Parse(layout, string(buf))
	if err != nil {
		return
	}

	_, err = r.Skip(len(buf))
	if err != nil {
		// Should never err
		return
	}

	return
}

// expectNextByteRead reads the next byte from r and checks whether it is exp.
// If a byte can be read but does not match exp, an error wrapping ErrFormat
// is returned. EOF is returned as ErrUnexpectedEOF.
func expectNextByteRead(r *fwd.Reader, exp byte) error {
	b, err := r.ReadByte()
	if err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		} else {
			return err
		}
	}
	if b != exp {
		return fmt.Errorf("read %#x but expected %#x: %w", b, exp, ErrFormat)
	}
	return nil
}

// peekUntilDelim peeks from r until delim is encountered or max byte are
// peeked.
// Returns a slice from r's internal buffer valid until the next reader
// method call. The delim is not included in the slice.
// err is io.EOF when EOF was encountered before delim and in this case buf
// contains all data until EOF. For all other errors, a nil buf is returned.
// err is ErrMaxLengthExceeded if max bytes do not contain delim.
func peekUntilDelim(r *fwd.Reader, delim byte, max int) (buf []byte, err error) {
	var n, off int

	if r.Buffered() > 0 {
		n = r.Buffered()
	} else if r.BufferSize() < max {
		n = r.BufferSize()
	} else {
		n = max
	}

	for {
		buf, err = r.Peek(n)
		if err != nil && err != io.EOF {
			return nil, err
		}
		// err == io.EOF is still kept for the next if block

		ind := bytes.IndexByte(buf[off:], delim)
		if ind != -1 {
			// Found delim; discard possible EOF as irrelevant
			return buf[:off+ind], nil
		} else if ind == -1 && err == io.EOF {
			return buf, io.EOF
		}

		if n == max {
			return nil, ErrMaxLengthExceeded
		}

		off = len(buf)

		if n < r.BufferSize() {
			n = r.BufferSize()
		} else {
			n *= 2
		}
		if n > max {
			n = max
		}
	}
}

// skipUntilDelim consumes from r until delim is encountered.
// The delim is not consumed.
// Returns the number of skipped bytes and an error, if one was encountered.
// io.EOF (and the number of skipped bytes) is returned as the error if EOF
// was encountered before delim.
// skipWhileCond is a more general version of this.
func skipUntilDelim(r *fwd.Reader, delim byte) (skipped int, err error) {
	var buf []byte
	var s int

	// Firstly only look at buffered data
	if r.Buffered() > 0 {
		buf, err = r.Peek(r.Buffered())
		if err != nil {
			// EOF should never be returned, so neglect err == io.EOF
			return
		}

		ind := bytes.IndexByte(buf, delim)
		if ind != -1 {
			skipped, err = r.Skip(ind)
			return
		} else {
			skipped, err = r.Skip(len(buf))
			if err != nil {
				// Should never err
				return
			}
		}
	}

	// Secondly read new data (Peek) until delim is found
	for {
		buf, err = r.Peek(r.BufferSize())
		if err != nil && err != io.EOF {
			return
		}

		ind := bytes.IndexByte(buf, delim)
		if ind != -1 {
			// Found delim; EOF after delim is irrelevant
			s, err = r.Skip(ind)
			skipped += s
			return
		} else if ind == -1 {
			if err == io.EOF {
				s, err = r.Skip(len(buf))
				skipped += s
				if err == nil {
					// Restore EOF as there was no real error
					err = io.EOF
				}
				return
			} else {
				s, err = r.Skip(len(buf))
				skipped += s
				if err != nil {
					// Should never err
					return
				}
			}
		}
	}
}

// isAsciiSpace returns whether b is an ASCII whitespace character.
func isAsciiSpace(b byte) bool {
	switch b {
	case '\t', '\n', '\v', '\f', '\r', ' ':
		return true
	}
	return false
}

// skipWhileCond consumes from r until condF is no longer met on the next byte.
// The !condF byte is not consumed.
// Returns the number of skipped bytes and an error, if one was encountered.
// io.EOF (and the number of skipped bytes) is returned as the error if EOF
// was encountered before delim.
// This is a more general version of skipUntilDelim.
func skipWhileCond(r *fwd.Reader, condF func(byte) bool) (skipped int, err error) {
	var buf []byte
	var s int

	// Firstly only look at buffered data
	if r.Buffered() > 0 {
		buf, err = r.Peek(r.Buffered())
		if err != nil {
			// EOF should never be returned, so neglect err == io.EOF
			return
		}

		var ind int = -1
		for i, b := range buf {
			if !condF(b) {
				ind = i
				break
			}
		}
		if ind != -1 {
			skipped, err = r.Skip(ind)
			return
		} else {
			skipped, err = r.Skip(len(buf))
			if err != nil {
				// Should never err
				return
			}
		}
	}

	// Secondly read new data (Peek) as long as condF is satisfied
	for {
		buf, err = r.Peek(r.BufferSize())
		if err != nil && err != io.EOF {
			return
		}

		var ind int = -1
		for i, b := range buf {
			if !condF(b) {
				ind = i
				break
			}
		}
		if ind != -1 {
			// Found delim; EOF after delim is irrelevant
			s, err = r.Skip(ind)
			skipped += s
			return
		} else if ind == -1 {
			if err == io.EOF {
				s, err = r.Skip(len(buf))
				skipped += s
				if err == nil {
					// Restore EOF as there was no real error
					err = io.EOF
				}
				return
			} else {
				s, err = r.Skip(len(buf))
				skipped += s
				if err != nil {
					// Should never err
					return
				}
			}
		}
	}
}

// noEOF returns err but replaces io.EOF with io.ErrUnexpectedEOF.
func noEOF(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	} else {
		return err
	}
}
