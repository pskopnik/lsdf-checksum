package lengthsafe

import (
	"os"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"

	"git.scc.kit.edu/sdm/lsdf-checksum/internal/osutils"
)

const (
	PathMax uint = 4096

	POSIXSymlinkMax uint = 255

	findBoundsBaseLength uint          = 256
	sampleStr            string        = "0123456789"
	symlinkProgressWait  time.Duration = 500 * time.Microsecond
)

var symlinkMax uint
var symlinkMaxIsSet uint32
var symlinkMaxInProgress uint32

func ensureSymlinkMaxSet() error {
	isSet := atomic.LoadUint32(&symlinkMaxIsSet)

	if isSet == 0 {
		inProgress := atomic.SwapUint32(&symlinkMaxInProgress, 1)
		if inProgress == 0 {
			err := findSymlinkMax()
			atomic.StoreUint32(&symlinkMaxInProgress, 0)
			return err
		} else {
			timer := time.NewTimer(symlinkProgressWait)

			for {
				<-timer.C

				inProgress = atomic.LoadUint32(&symlinkMaxInProgress)
				if inProgress == 0 {
					break
				}

				timer.Reset(symlinkProgressWait)
			}
		}
	}

	return nil
}

func findSymlinkMax() error {
	var ok bool
	var tryLength uint

	lower, upper, err := findBounds()
	if err != nil {
		return err
	}

	tryLength, ok, err = searchDirectional(upper, false, 2)
	if err != nil {
		return err
	}
	if ok {
		symlinkMax = tryLength
		return nil
	}
	tryLength, ok, err = searchDirectional(lower, true, 2)
	if err != nil {
		return err
	}
	if ok {
		symlinkMax = tryLength
		return nil
	}

	tryLength, ok, err = searchBinary(lower, upper)
	if err != nil {
		return err
	}
	if ok {
		symlinkMax = tryLength
		return nil
	}

	return nil
}

// findBounds returns a lower and upper bound for SYMLINK_MAX.
// The function will try exponentially-increasing symlink lengths.
// lower is the greatest still accepted symlink length, upper is the lowest
// rejected symlink length.
// lower <= SYMLINK_MAX < upper
func findBounds() (lower uint, upper uint, err error) {
	var ok bool

	lower = uint(0)
	upper = findBoundsBaseLength

	for {
		ok, err = trySymlinkLength(upper)
		if err != nil {
			return 0, 0, err
		}
		if !ok {
			return lower, upper, nil
		}

		lower = upper
		upper *= 2
	}
}

func searchDirectional(length uint, forward bool, n uint) (uint, bool, error) {
	var tryLength, stepOffset int
	if forward {
		stepOffset = 1
	} else {
		stepOffset = -1
	}

	tryLength = int(length)

	for i := uint(1); i <= n; i++ {
		tryLength += stepOffset

		ok, err := trySymlinkLength(uint(tryLength))
		if err != nil {
			return 0, false, err
		}
		if ok != forward {
			if forward {
				tryLength--
			}
			return uint(tryLength), true, nil
		}
	}

	return 0, false, nil
}

func searchBinary(start, end uint) (uint, bool, error) {
	var tryLength uint

	for {
		if end-start <= 1 {
			break
		}
		tryLength = start + (end-start)/2

		ok, err := trySymlinkLength(tryLength)
		if err != nil {
			return 0, false, err
		}
		if ok {
			start = tryLength
		} else {
			end = tryLength
		}
	}

	return start, true, nil
}

func trySymlinkLength(length uint) (bool, error) {
	// The length calculation is: ceil(length / |sampleStr|)
	oldname := strings.Repeat(sampleStr, ((int(length)-1)/len(sampleStr))+1)[:int(length)]

	name, err := osutils.CreateTempSymlink(oldname, lengthsafePrefix, "", "")
	if err != nil {
		if isNameTooLong(err) {
			return false, nil
		}

		return false, err
	}

	_ = os.Remove(name)

	return true, nil
}

func isNameTooLong(err error) bool {
	if linkErr, ok := err.(*os.LinkError); ok {
		if linkErr.Err == unix.ENAMETOOLONG {
			return true
		}
	}

	return false
}
