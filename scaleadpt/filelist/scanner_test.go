package filelist_test

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt/filelist"
)

var _ = Describe("Scanner", func() {

	Describe("ParseInt64", func() {
		It("should parse integers while respecting the allowWhitespace parameter", func() {
			scanner := setupScanner("123")
			Ω(scanner.ParseInt64(false)).Should(Equal(int64(123)))

			scanner = setupScanner("123 ")
			Ω(scanner.ParseInt64(false)).Should(Equal(int64(123)))

			scanner = setupScanner("   123")
			Ω(scanner.ParseInt64(true)).Should(Equal(int64(123)))

			scanner = setupScanner("123AB")
			Ω(scanner.ParseInt64(true)).Should(Equal(int64(123)))
		})

		It("should return an error if there is leading whitespace", func() {
			scanner := setupScanner(" 123")
			_, err := scanner.ParseInt64(false)
			Ω(err).Should(HaveOccurred())
		})

		It("should return an error if the input does not start with an integer", func() {
			inputs := []string{
				"A123",
				"|123",
				"| 123",
			}

			var scanner Scanner
			var err error

			for _, input := range inputs {
				scanner = setupScanner(input)
				_, err = scanner.ParseInt64(false)
				Ω(err).Should(HaveOccurred())
			}

			for _, input := range inputs {
				scanner = setupScanner(input)
				_, err = scanner.ParseInt64(true)
				Ω(err).Should(HaveOccurred())
			}
		})
	})
})

func setupScanner(content string) Scanner {
	var scanner Scanner

	r := strings.NewReader(content)
	scanner.Init(r)

	return scanner
}
