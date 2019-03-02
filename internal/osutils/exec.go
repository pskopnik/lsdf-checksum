package osutils

import (
	"os/exec"
)

func IsExecNotFound(err error) bool {
	if execErr, ok := err.(*exec.Error); ok {
		return execErr.Err == exec.ErrNotFound
	}

	return false
}
