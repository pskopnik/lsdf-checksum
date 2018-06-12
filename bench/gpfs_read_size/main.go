package main

import (
	"fmt"
	"io"
	"os"
	"time"
)

func main() {
	r, err := os.Open("/gpfs2/largetestfile")
	if err != nil {
		panic(err)
	}
	defer r.Close()

	w, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	defer w.Close()

	sizes := []int{
		1 * 1024,
		2 * 1024,
		4 * 1024,
		8 * 1024,
		16 * 1024,
		32 * 1024,
		64 * 1024,
		128 * 1024,
		256 * 1024,
		512 * 1024,
		1024 * 1024,
		2048 * 1024,
	}

	for i := 0; i < 10; i++ {
		for _, size := range sizes {
			_, err := r.Seek(0, 0)
			if err != nil {
				panic(err)
			}

			start := time.Now()

			n, err := io.CopyBuffer(w, r, make([]byte, size))
			if err != nil {
				panic(err)
			}

			end := time.Now()
			fmt.Println(n, size, end.Sub(start).Seconds())
		}
	}

}
