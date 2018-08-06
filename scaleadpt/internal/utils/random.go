package utils

import (
	"math/rand"
	"os"
	"time"
)

var ownRand *rand.Rand

func prepareRand() {
	if ownRand == nil {
		src := rand.NewSource(time.Now().UnixNano() + int64(os.Getpid()))
		ownRand = rand.New(src)
	}
}

func RandomString(length int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	prepareRand()

	buf := make([]byte, length)

	for i := 0; i < length; i++ {
		buf[i] = letters[ownRand.Intn(len(letters))]
	}

	return string(buf)
}
