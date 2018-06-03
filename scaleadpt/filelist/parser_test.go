package filelist_test

import (
	"io"
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt/filelist"
)

var _ = Describe("Parser", func() {
	Describe("ParseLine()", func() {
		It("should parse a single line", func() {
			parser := setupParser("1 2 3 |4|2018-05-22 23:01:39.727768| -- /some/file/path")

			modTime, err := time.Parse("2006-01-02 15:04:05", "2018-05-22 23:01:39.727768")
			Ω(err).ShouldNot(HaveOccurred())

			Ω(parser.ParseLine()).Should(Equal(&FileData{
				Inode:            1,
				Generation:       2,
				SnapshotId:       3,
				Filesize:         4,
				ModificationTime: modTime,
				Path:             "/some/file/path",
			}))
		})

		It("should parse a second line", func() {
			parser := setupParser("1 2 3 |4|2018-05-22 22:01:39.727768| -- /some/file/path\n2 3 4 |5|2017-04-21 22:00:38.322244| -- /another/file/path")

			modTime, err := time.Parse("2006-01-02 15:04:05", "2018-05-22 22:01:39.727768")
			Ω(err).ShouldNot(HaveOccurred())

			Ω(parser.ParseLine()).Should(Equal(&FileData{
				Inode:            1,
				Generation:       2,
				SnapshotId:       3,
				Filesize:         4,
				ModificationTime: modTime,
				Path:             "/some/file/path",
			}))

			modTime, err = time.Parse("2006-01-02 15:04:05", "2017-04-21 22:00:38.322244")
			Ω(err).ShouldNot(HaveOccurred())

			Ω(parser.ParseLine()).Should(Equal(&FileData{
				Inode:            2,
				Generation:       3,
				SnapshotId:       4,
				Filesize:         5,
				ModificationTime: modTime,
				Path:             "/another/file/path",
			}))
		})

		It("should parse filenames with spaces and symbols", func() {
			parser := setupParser("1 2 3 |4|2018-05-22 23:01:39.727768| -- /this is a/file /path.with/&a_lot-of/%strange (characters).?ß")

			modTime, err := time.Parse("2006-01-02 15:04:05", "2018-05-22 23:01:39.727768")
			Ω(err).ShouldNot(HaveOccurred())

			Ω(parser.ParseLine()).Should(Equal(&FileData{
				Inode:            1,
				Generation:       2,
				SnapshotId:       3,
				Filesize:         4,
				ModificationTime: modTime,
				Path:             "/this is a/file /path.with/&a_lot-of/%strange (characters).?ß",
			}))
		})

		It("should return io.EOF at eof", func() {
			parser := setupParser("1 2 3 |4|2018-05-22 22:01:39.727768| -- /some/file/path")

			_, err := parser.ParseLine()
			Ω(err).ShouldNot(HaveOccurred())

			_, err = parser.ParseLine()
			Ω(err).Should(Equal(io.EOF))
		})

		It("should return io.EOF when the input is empty", func() {
			parser := setupParser("")

			_, err := parser.ParseLine()
			Ω(err).Should(Equal(io.EOF))
		})

		It("should return io.EOF when a tailing empty line is encountered", func() {
			parser := setupParser("1 2 3 |4|2018-05-22 22:01:39.727768| -- /some/file/path\n")

			_, err := parser.ParseLine()
			Ω(err).ShouldNot(HaveOccurred())

			_, err = parser.ParseLine()
			Ω(err).Should(Equal(io.EOF))
		})

	})
})

func setupParser(input string) *Parser {
	r := strings.NewReader(input)

	return NewParser(r)
}
