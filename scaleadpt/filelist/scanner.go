package filelist

import (
	"errors"
	"strconv"
	"text/scanner"
	"time"
	"unicode"
)

var (
	ErrUnexpectedFormat = errors.New("Unexpteced format encountered.")
)

const (
	delimitingWhitespace = 1<<'\t' | 1<<' '

	eol = '\n'
)

func isDateRune(r rune, _ int) bool {
	return unicode.IsDigit(r) || r == ':' || r == ' ' || r == '.' || r == '-' || unicode.IsLetter(r) || r == ','
}

type Scanner struct {
	scanner.Scanner
}

func (s *Scanner) ParseInt64(allowWhitespace bool) (int64, error) {
	oldMode := s.Mode
	oldWhitespace := s.Whitespace

	s.Mode = scanner.ScanInts
	if allowWhitespace {
		s.Whitespace = delimitingWhitespace
	} else {
		s.Whitespace = 0
	}
	defer func() {
		s.Mode = oldMode
		s.Whitespace = oldWhitespace
	}()

	if s.Scan() != scanner.Int {
		return 0, ErrUnexpectedFormat
	}

	parsedInt, err := strconv.ParseInt(s.TokenText(), 10, 64)
	if err != nil {
		return 0, err
	}

	return parsedInt, nil
}

func (s *Scanner) ParseUint64(allowWhitespace bool) (uint64, error) {
	oldMode := s.Mode
	oldWhitespace := s.Whitespace

	s.Mode = scanner.ScanInts
	if allowWhitespace {
		s.Whitespace = delimitingWhitespace
	} else {
		s.Whitespace = 0
	}
	defer func() {
		s.Mode = oldMode
		s.Whitespace = oldWhitespace
	}()

	if s.Scan() != scanner.Int {
		return 0, ErrUnexpectedFormat
	}

	parsedInt, err := strconv.ParseUint(s.TokenText(), 10, 64)
	if err != nil {
		return 0, err
	}

	return parsedInt, nil
}

// ParseTime parses a timestamp.
// time.Parse() is used for the actual parsing.
// The parameter layout is passed on to time.Parse() If layout is empty,
// time.ANSIC is used instead.
// The parameter loc is used as the location in calls to
// time.ParseInLocation(). If loc is nil, time.Parse() is used instead.
// Parsing will fail if the input time contains any characters which are not
// alphanumeric characters, ':', ' ', '.', '-' or ','.
func (s *Scanner) ParseTime(layout string, loc *time.Location) (time.Time, error) {
	oldMode := s.Mode
	oldWhitespace := s.Whitespace
	oldIsIdentRune := s.IsIdentRune

	s.Mode = scanner.ScanIdents
	s.Whitespace = 0
	s.IsIdentRune = isDateRune
	defer func() {
		s.Mode = oldMode
		s.Whitespace = oldWhitespace
		s.IsIdentRune = oldIsIdentRune
	}()

	if s.Scan() != scanner.Ident {
		return time.Time{}, ErrUnexpectedFormat
	}

	if len(layout) == 0 {
		layout = time.ANSIC
	}

	if loc != nil {
		return time.ParseInLocation(
			layout,
			s.TokenText(),
			loc,
		)
	} else {
		return time.Parse(
			layout,
			s.TokenText(),
		)
	}
}

func (s *Scanner) ExpectRune(r rune, allowWhitespace bool) error {
	oldMode := s.Mode
	oldWhitespace := s.Whitespace

	s.Mode = 0
	if allowWhitespace {
		s.Whitespace = delimitingWhitespace
	} else {
		s.Whitespace = 0
	}
	defer func() {
		s.Mode = oldMode
		s.Whitespace = oldWhitespace
	}()

	if s.Scan() != r {
		return ErrUnexpectedFormat
	}

	return nil
}

// SkipWhitespace consumes all runes which are classified as whitespace by the
// whitespace value passed in.
// The whitespace value is interpreted exactly like s.Whitespace.
func (s *Scanner) SkipWhitespace(whitespace uint64) error {
	for r := s.Peek(); whitespace&(1<<uint(r)) != 0; r = s.Peek() {
		s.Next()
	}

	return nil
}

// SkipToEOL consumes all runes until an EOL rune ('\n') is encountered.
// The EOL itself is consumed as well.
// SkipToEOL also stops when the EOF is encountered.
func (s *Scanner) SkipToEOL() {
	for r := s.Peek(); r != eol && r != scanner.EOF; r = s.Peek() {
		s.Next()
	}
	s.Next()
}
