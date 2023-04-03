package filelist_test

import (
	"bytes"
	"io"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt/filelist"
)

var _ = Describe("Parser", func() {
	Describe("ParseLine()", func() {
		It("should parse a single line", func() {
			var fileData FileData
			parser := setupParser("1 2 3 |4|2018-05-22 23:01:39.727768| -- /some/file/path")

			modTime, err := time.Parse("2006-01-02 15:04:05", "2018-05-22 23:01:39.727768")
			Ω(err).ShouldNot(HaveOccurred())

			Ω(parser.ParseLine(&fileData)).ShouldNot(HaveOccurred())
			Ω(fileData).Should(Equal(FileData{
				Inode:            1,
				Generation:       2,
				SnapshotID:       3,
				FileSize:         4,
				ModificationTime: modTime,
				Path:             "/some/file/path",
			}))
		})

		It("should skip over extra spaces after preamble", func() {
			var fileData FileData
			parser := setupParser("1 2 3  |4|2018-05-22 23:01:39.727768| -- /some/file/path")

			Ω(parser.ParseLine(&fileData)).ShouldNot(HaveOccurred())
		})

		It("should parse filenames with spaces and symbols", func() {
			var fileData FileData
			parser := setupParser("1 2 3 |4|2018-05-22 23:01:39.727768| -- /this%20is a/file /path.wi%0Ath/&a_lot-of/%25strange (characters).?ß")

			modTime, err := time.Parse("2006-01-02 15:04:05", "2018-05-22 23:01:39.727768")
			Ω(err).ShouldNot(HaveOccurred())

			Ω(parser.ParseLine(&fileData)).ShouldNot(HaveOccurred())
			Ω(fileData).Should(Equal(FileData{
				Inode:            1,
				Generation:       2,
				SnapshotID:       3,
				FileSize:         4,
				ModificationTime: modTime,
				Path:             "/this is a/file /path.wi\nth/&a_lot-of/%strange (characters).?ß",
			}))
		})

		It("should return io.EOF at eof", func() {
			var fileData FileData
			parser := setupParser("1 2 3 |4|2018-05-22 22:01:39.727768| -- /some/file/path")

			Ω(parser.ParseLine(&fileData)).ShouldNot(HaveOccurred())

			Ω(parser.ParseLine(&fileData)).Should(Equal(io.EOF))
		})

		It("should return io.EOF when the input is empty", func() {
			var fileData FileData
			parser := setupParser("")

			Ω(parser.ParseLine(&fileData)).Should(Equal(io.EOF))
		})

		It("should return io.EOF when a tailing empty line is encountered", func() {
			var fileData FileData
			parser := setupParser("1 2 3 |4|2018-05-22 22:01:39.727768| -- /some/file/path\n")

			Ω(parser.ParseLine(&fileData)).ShouldNot(HaveOccurred())

			Ω(parser.ParseLine(&fileData)).Should(Equal(io.EOF))
		})

		It("should parse multi-line input completely", func() {
			var fileData FileData
			parser := setupParser("1 2 3 |4|2018-05-22 22:01:39.727768| -- /some/file/path\n2 3 4 |5|2017-04-21 22:00:38.322244| -- /another/file/path")

			modTime, err := time.Parse("2006-01-02 15:04:05", "2018-05-22 22:01:39.727768")
			Ω(err).ShouldNot(HaveOccurred())

			Ω(parser.ParseLine(&fileData)).ShouldNot(HaveOccurred())
			Ω(fileData).Should(Equal(FileData{
				Inode:            1,
				Generation:       2,
				SnapshotID:       3,
				FileSize:         4,
				ModificationTime: modTime,
				Path:             "/some/file/path",
			}))

			modTime, err = time.Parse("2006-01-02 15:04:05", "2017-04-21 22:00:38.322244")
			Ω(err).ShouldNot(HaveOccurred())

			Ω(parser.ParseLine(&fileData)).ShouldNot(HaveOccurred())
			Ω(fileData).Should(Equal(FileData{
				Inode:            2,
				Generation:       3,
				SnapshotID:       4,
				FileSize:         5,
				ModificationTime: modTime,
				Path:             "/another/file/path",
			}))

			Ω(parser.ParseLine(&fileData)).Should(Equal(io.EOF))
		})

		It("should parse larger inputs", func() {
			var fileData FileData
			lines := 500
			input := "3945752 40651 538 |10939464|2018-05-22 22:01:39.727768| -- /some/file/path/with/significant/length/i%0At/goes/fur%20ther/and/further\n"
			parser := setupParser(input, lines)

			modTime, err := time.Parse("2006-01-02 15:04:05", "2018-05-22 22:01:39.727768")
			Ω(err).ShouldNot(HaveOccurred())

			for i := 0; i < lines; i++ {
				Ω(parser.ParseLine(&fileData)).ShouldNot(HaveOccurred())
				Ω(fileData).Should(Equal(FileData{
					Inode:            3945752,
					Generation:       40651,
					SnapshotID:       538,
					FileSize:         10939464,
					ModificationTime: modTime,
					Path:             "/some/file/path/with/significant/length/i\nt/goes/fur ther/and/further",
				}))
			}

			Ω(parser.ParseLine(&fileData)).Should(Equal(io.EOF))
		})
	})
})

func setupParser(input string, repeats ...int) *Parser {
	n := 1
	if len(repeats) > 0 {
		n = repeats[0]
	}

	fullInput := bytes.Repeat([]byte(input), n)
	r := bytes.NewReader(fullInput)

	return NewParser(r)
}

func BenchmarkParser(b *testing.B) {
	input := "3945752 40651 538 |10939464|2018-05-22 22:01:39.727768| -- /some/file/path/with/significant/length/i%0At/goes/fur%20ther/and/further\n"
	parser := setupParser(input, b.N)
	var totalFileSize uint64

	b.ResetTimer()
	for {
		var fileData FileData
		err := parser.ParseLine(&fileData)
		if err == io.EOF {
			break
		} else if err != nil {
			b.Error(err)
			return
		}
		totalFileSize += fileData.FileSize
	}

	b.SetBytes(int64(len(input)))
}
