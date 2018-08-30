package main

import (
	"crypto/sha1"
	"fmt"
	"io"
	"os"
)

const bufSize = 32 * 1024

func main() {
	var err error
	var in io.Reader
	var name string

	if len(os.Args) == 1 || os.Args[1] == "-" {
		in = os.Stdin
		name = "-"
	} else {
		f, err := os.Open(os.Args[1])
		if err != nil {
			panic(err)
		}
		defer f.Close()

		in = f
		name = os.Args[1]
	}

	buf := make([]byte, bufSize)

	hasher := sha1.New()

	_, err = io.CopyBuffer(hasher, in, buf)
	if err != nil {
		panic(err)
	}

	var sumBytes [sha1.Size]byte
	sum := hasher.Sum(sumBytes[:0])

	fmt.Printf("%x\t%s\n", sum, name)
}
