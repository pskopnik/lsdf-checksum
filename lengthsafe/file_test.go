package lengthsafe

import (
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("splitPathOnSymlinkLimit()", func() {
	BeforeEach(func() {
		ensureSymlinkMaxSet()
	})

	Context("when path is shorter than symlinkMax", func() {
		It("should return dir and remainder", func() {
			for _, path := range []string{
				pathStr(5),
				pathStr(symlinkMax),
			} {
				dir, remainder := splitPathOnSymlinkLimit(path)

				Ω(dir).Should(Equal(path))
				Ω(remainder).Should(Equal(""))
			}
		})
	})

	Context("when path[:symlinkMax] splits the final file name", func() {
		It("should return dir and remainder", func() {
			path := pathStr(symlinkMax) + "asdf"

			dir, remainder := splitPathOnSymlinkLimit(path)

			Ω(dir).Should(Equal(filepath.Dir(path)))
			Ω(remainder).Should(Equal(filepath.Base(path)))
		})
	})

	Context("when path[:symlinkMax] splits a dir name", func() {
		It("should return dir and remainder", func() {
			path := pathStr(symlinkMax) + "asdf/asdf"

			dir, remainder := splitPathOnSymlinkLimit(path)

			Ω(dir).Should(Equal(filepath.Dir(filepath.Dir(path))))
			Ω(remainder).Should(Equal(filepath.Base(filepath.Dir(path)) + "/" + filepath.Base(path)))
		})
	})

	Context("when path[:symlinkMax] ends on '/'", func() {
		It("should return dir and remainder", func() {
			path := pathStr(symlinkMax-1) + "/asdf"

			dir, remainder := splitPathOnSymlinkLimit(path)

			Ω(dir).Should(Equal(filepath.Dir(path)))
			Ω(remainder).Should(Equal(filepath.Base(path)))
		})
	})
})

// pathStr returns a string of length which looks like a filesystem path.
// This path will never end with a '/'.
func pathStr(length uint) string {
	const sampleStr = "/0123456789"

	if length == 0 {
		return ""
	}

	// The length calculation is: ceil(length / |sampleStr|)
	s := strings.Repeat(sampleStr, (int(length-1)/len(sampleStr))+1)[:length]

	if s[len(s)-1] == '/' {
		s = s[:len(s)-1] + "a"
	}

	return s
}
