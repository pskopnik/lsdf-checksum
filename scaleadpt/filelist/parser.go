package filelist

import (
	"errors"
	"io"
	"text/scanner"
	"time"
)

type FileData struct {
	Inode            int64
	Generation       int64
	SnapshotId       int64
	FileSize         int64
	ModificationTime time.Time
	Path             string
}

const (
	timeLayout = "2006-01-02 15:04:05"
)

type Parser struct {
	r io.Reader
	s Scanner

	// Loc is used as the location in calls to time.ParseInLocation().
	// If Loc is nil, time.Parse() is used instead.
	Loc *time.Location
}

// NewParser constructs a new Parser instance using the passed in io.Reader.
// When reading from a file, it is sensible to wrap the file in a bufio.Reader.
func NewParser(r io.Reader) *Parser {
	parser := &Parser{
		r: r,
		s: Scanner{},
	}

	parser.s.Init(r)
	parser.s.Whitespace = 0
	parser.s.Mode = 0

	return parser
}

// ParseLine parses a single line and returns the extracted data as a FileData
// object.
// When the end of the reader is reached, (nil, io.EOF) is returned.
func (p *Parser) ParseLine() (*FileData, error) {
	var err, scannerError error

	if p.s.Peek() == scanner.EOF {
		return nil, io.EOF
	}

	fileData := &FileData{}

	p.s.Error = func(_ *scanner.Scanner, msg string) {
		scannerError = errors.New(msg)
	}

	err = p.parsePreamble(fileData)
	if err != nil {
		return nil, err
	} else if scannerError != nil {
		return nil, scannerError
	}

	err = p.parseCustomAttributes(fileData)
	if err != nil {
		return nil, err
	} else if scannerError != nil {
		return nil, scannerError
	}

	err = p.parseFilenameSeparator()
	if err != nil {
		return nil, err
	} else if scannerError != nil {
		return nil, scannerError
	}

	err = p.parseFilename(fileData)
	if err != nil {
		return nil, err
	} else if scannerError != nil {
		return nil, scannerError
	}

	next := p.s.Next()
	if next != eol && next != scanner.EOF {
		return nil, UnexpectedFormatErr
	}

	return fileData, nil
}

func (p *Parser) parsePreamble(fileData *FileData) error {
	var err error

	fileData.Inode, err = p.s.ParseInt64(true)
	if err != nil {
		return err
	}

	fileData.Generation, err = p.s.ParseInt64(true)
	if err != nil {
		return err
	}

	fileData.SnapshotId, err = p.s.ParseInt64(true)
	if err != nil {
		return err
	}

	return nil
}

func (p *Parser) parseCustomAttributes(fileData *FileData) error {
	var err error

	err = p.s.ExpectRune('|', true)
	if err != nil {
		return err
	}

	p.s.Mode = scanner.ScanInts
	p.s.Whitespace = 0

	fileData.FileSize, err = p.s.ParseInt64(false)
	if err != nil {
		return err
	}

	err = p.s.ExpectRune('|', false)
	if err != nil {
		return err
	}

	fileData.ModificationTime, err = p.s.ParseTime(timeLayout, p.Loc)
	if err != nil {
		return err
	}

	err = p.s.ExpectRune('|', false)
	if err != nil {
		return err
	}

	return nil
}

func (p *Parser) parseFilename(fileData *FileData) error {
	p.s.Mode = scanner.ScanIdents
	p.s.Whitespace = 0
	p.s.IsIdentRune = func(r rune, _ int) bool {
		return r != eol && r != scanner.EOF
	}
	defer func() {
		p.s.IsIdentRune = nil
	}()

	if p.s.Scan() != scanner.Ident {
		return UnexpectedFormatErr
	}

	fileData.Path = p.s.TokenText()

	return nil
}

func (p *Parser) parseFilenameSeparator() error {
	var err error

	err = p.s.ExpectRune('-', true)
	if err != nil {
		return err
	}
	err = p.s.ExpectRune('-', false)
	if err != nil {
		return err
	}

	err = p.s.SkipWhitespace(delimitingWhitespace)
	if err != nil {
		return err
	}

	return nil
}
