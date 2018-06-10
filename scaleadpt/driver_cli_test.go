package scaleadpt

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("CLIDriver", func() {
	Describe("parseSnapdirOutput", func() {
		// These output have been compiled from actual output observed as well
		// as the reference:
		// https://www.ibm.com/support/knowledgecenter/en/STXKQY_4.2.3/com.ibm.spectrum.scale.v4r23.doc/bl1adm_mmsnapdir.htm

		It("should parse one line output", func() {
			const (
				fs1Output = `Snapshot directory for "fs1" is ".link" (root directory only)
`
				gpfs1Output = `Snapshot directory for "gpfs1" is ".snaps" (all directories)
`
			)

			driver := &CLIDriver{}

			Ω(driver.parseSnapdirOutput([]byte(fs1Output), "fs1")).Should(Equal(&SnapshotDirsInfo{
				Global:           ".link",
				GlobalsInFileset: false,
				Fileset:          ".link",
				AllDirectories:   false,
			}))

			Ω(driver.parseSnapdirOutput([]byte(gpfs1Output), "gpfs1")).Should(Equal(&SnapshotDirsInfo{
				Global:           ".snaps",
				GlobalsInFileset: false,
				Fileset:          ".snaps",
				AllDirectories:   true,
			}))
		})

		It("should parse two line output", func() {
			const (
				gpfs2Output = `Fileset snapshot directory for "gpfs2" is ".snapshots" (root directory only)
Global snapshot directory for "gpfs2" is ".snapshots" in root fileset
`
				gpfs3Output = `Fileset snapshot directory for "gpfs3" is ".snapshots" (root directory only)
Global snapshot directory for "gpfs3" is ".snapshots" in all filesets
`
				gpfs4Output = `Fileset snapshot directory for "gpfs4" is ".fsnaps" (all directories)
Global snapshot directory for "gpfs4" is ".gsnaps" in root fileset
`
				gpfs5Output = `Fileset snapshot directory for "gpfs5" is ".link" (all directories)
Global snapshot directory for "gpfs5" is ".link" in all filesets
`
			)

			driver := &CLIDriver{}

			Ω(driver.parseSnapdirOutput([]byte(gpfs2Output), "gpfs2")).Should(Equal(&SnapshotDirsInfo{
				Global:           ".snapshots",
				GlobalsInFileset: false,
				Fileset:          ".snapshots",
				AllDirectories:   false,
			}))

			Ω(driver.parseSnapdirOutput([]byte(gpfs3Output), "gpfs3")).Should(Equal(&SnapshotDirsInfo{
				Global:           ".snapshots",
				GlobalsInFileset: true,
				Fileset:          ".snapshots",
				AllDirectories:   false,
			}))

			Ω(driver.parseSnapdirOutput([]byte(gpfs4Output), "gpfs4")).Should(Equal(&SnapshotDirsInfo{
				Global:           ".gsnaps",
				GlobalsInFileset: false,
				Fileset:          ".fsnaps",
				AllDirectories:   true,
			}))

			Ω(driver.parseSnapdirOutput([]byte(gpfs5Output), "gpfs5")).Should(Equal(&SnapshotDirsInfo{
				Global:           ".link",
				GlobalsInFileset: true,
				Fileset:          ".link",
				AllDirectories:   true,
			}))
		})

		It("should return an error if the file system name does not match", func() {
			const (
				gpfs1Output = `Snapshot directory for "gpfs1" is ".snaps" (all directories)
`
				gpfs2Output = `Fileset snapshot directory for "gpfs2" is ".snapshots" (root directory only)
Global snapshot directory for "gpfs2" is ".snapshots" in root fileset
`
			)

			driver := &CLIDriver{}

			snapshotDirsInfo, err := driver.parseSnapdirOutput([]byte(gpfs1Output), "gpfs2")
			Ω(snapshotDirsInfo).Should(BeNil())
			Ω(err).Should(HaveOccurred())

			snapshotDirsInfo, err = driver.parseSnapdirOutput([]byte(gpfs2Output), "gpfs3")
			Ω(snapshotDirsInfo).Should(BeNil())
			Ω(err).Should(HaveOccurred())
		})

	})
})
