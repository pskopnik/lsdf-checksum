package filelist

import (
	"bytes"
	"errors"
)

var (
	errInvalidEscape = errors.New("invalid escape sequence")
)

func hexToInt(in byte) uint8 {
	switch {
	case '0' <= in && in <= '9':
		return in - '0'
	case 'a' <= in && in <= 'f':
		return in - 'a' + 10
	case 'A' <= in && in <= 'F':
		return in - 'A' + 10
	}
	return 0
}

func appendUnescapeUrl(dst, in []byte) ([]byte, error) {
	for {
		ind := bytes.IndexByte(in, '%')
		if ind == -1 {
			dst = append(dst, in...)
			return dst, nil
		}

		dst = append(dst, in[:ind]...)
		if ind + 2 >= len(in) {
			return dst, errInvalidEscape
		}
		dst = append(dst, hexToInt(in[ind+1])<<4 + hexToInt(in[ind+2]))

		in = in[ind+3:]
	}
}
