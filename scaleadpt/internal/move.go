package internal

import (
	"os"

	"github.com/hacdias/fileutils"
)

func MoveFile(src, dst string) error {
	err := os.Rename(src, dst)
	if err != nil {
		err = fileutils.CopyFile(src, dst)
		if err != nil {
			return err
		}

		err = os.Remove(src)
		if err != nil {
			_ = os.Remove(dst)
			return err
		}
	}

	return nil
}
