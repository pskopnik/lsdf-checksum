package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt"
	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt/filelist"
)

type Spec struct {
	Filesystems map[string]SpecFilesystem `yaml:"filesystems"`
}

type SpecFilesystem struct {
	Name                    string               `yaml:"name"`
	MountRoot               string               `yaml:"mount_root"`
	Version                 string               `yaml:"version"`
	SnapshotDirsInfo        SpecSnapshotDirsInfo `yaml:"snapshot_dirs_info"`
	CreateGetDeleteSnapshot string               `yaml:"create_get_delete_snapshot"`
	ListPolicy              SpecListPolicy       `yaml:"list_policy"`
}

type SpecSnapshotDirsInfo struct {
	Global           string `yaml:"global"`
	GlobalsInFileset bool   `yaml:"globals_in_fileset"`
	Fileset          string `yaml:"fileset"`
	AllDirectories   bool   `yaml:"all_directories"`
}

type SpecListPolicy struct {
	Subpath       string               `yaml:"subpath"`
	Files         []SpecListPolicyFile `yaml:"files"`
	FilesComplete bool                 `yaml:"files_complete"`
}

type SpecListPolicyFile struct {
	Path string `yaml:"path"`
	Size int64  `yaml:"size"`
}

type MismatchError struct {
	Name     string
	Observed interface{}
	Specced  interface{}
}

func (m *MismatchError) Error() string {
	return fmt.Sprintf(
		"Mismatch for \"%s\" between observed value \"%v\" and specced value \"%v\"",
		m.Name, m.Observed, m.Specced,
	)
}

func ShouldEqualString(obsVal, specVal string, name string) error {
	if obsVal != specVal {
		return &MismatchError{
			Observed: obsVal,
			Specced:  specVal,
			Name:     name,
		}
	}
	return nil
}

func ShouldEqualInt(obsVal, specVal int, name string) error {
	if obsVal != specVal {
		return &MismatchError{
			Observed: obsVal,
			Specced:  specVal,
			Name:     name,
		}
	}
	return nil
}

func ShouldEqualBool(obsVal, specVal bool, name string) error {
	if obsVal != specVal {
		return &MismatchError{
			Observed: obsVal,
			Specced:  specVal,
			Name:     name,
		}
	}
	return nil
}

func ShouldApproxEqualTime(obsVal, specVal time.Time, intv time.Duration, name string) error {
	windowStart := specVal.Add(-intv)
	windowEnd := specVal.Add(intv)

	if obsVal.Before(windowStart) || obsVal.After(windowEnd) {
		return &MismatchError{
			Observed: obsVal,
			Specced:  specVal,
			Name:     name,
		}
	}
	return nil
}

type Reporter struct {
	modules map[string]*reporterModule
}

type reporterModule struct {
	skipped  bool
	runtime  []error
	mismatch []error
}

func (r *Reporter) ensureAndGetModule(module string) *reporterModule {
	if r.modules == nil {
		r.modules = make(map[string]*reporterModule)
	}
	if m, ok := r.modules[module]; ok {
		return m
	}
	m := &reporterModule{}
	r.modules[module] = m
	return m
}

func (r *Reporter) PrintSummary() {
	fmt.Println("Summary")

	names := make([]string, 0, len(r.modules))

	for name, m := range r.modules {
		if len(m.runtime)+len(m.mismatch) == 0 && !m.skipped {
			names = append(names, name)
		}
	}
	if len(names) > 0 {
		fmt.Printf("  SUCCEEDED %d modules: %s\n", len(names), strings.Join(names, ", "))
	}

	names = names[:0]

	for name, m := range r.modules {
		if len(m.runtime)+len(m.mismatch) == 0 && m.skipped {
			names = append(names, name)
		}
	}
	if len(names) > 0 {
		fmt.Printf("  SKIPPED %d modules: %s\n", len(names), strings.Join(names, ", "))
	}

	names = names[:0]

	for name, m := range r.modules {
		if len(m.runtime)+len(m.mismatch) > 0 {
			names = append(names, name)
		}
	}
	if len(names) > 0 {
		fmt.Printf("  FAILED %d modules: %s\n", len(names), strings.Join(names, ", "))
	}
}

func (r *Reporter) IsSuccessful() bool {
	for _, m := range r.modules {
		if len(m.runtime)+len(m.mismatch) > 0 {
			return false
		}
	}

	return true
}

func (r *Reporter) StartModule(module string) ReporterModuleCtx {
	fmt.Printf("module %s starts\n", module)

	m := r.ensureAndGetModule(module)

	return ReporterModuleCtx{
		name: module,
		data: m,
	}
}

type ReporterModuleCtx struct {
	name string
	data *reporterModule
}

func (r ReporterModuleCtx) AddRuntimeErr(errs ...error) {
	for _, err := range errs {
		if err == nil {
			continue
		}

		fmt.Printf("runtime error in module %s: %v\n", r.name, err)

		r.data.runtime = append(r.data.runtime, err)
	}
}

func (r ReporterModuleCtx) AddMismatchErr(errs ...error) {
	for _, err := range errs {
		if err == nil {
			continue
		}

		fmt.Printf("mismatch error in module %s: %v\n", r.name, err)

		r.data.mismatch = append(r.data.mismatch, err)
	}
}

func (r ReporterModuleCtx) ReportSkipped() {
	fmt.Printf("module %s is skipped and does not perform its checks (skipped) \n", r.name)
	r.data.skipped = true
}

func (r ReporterModuleCtx) Finish() {
	if len(r.data.runtime)+len(r.data.mismatch) > 0 {
		fmt.Printf("module %s finished with errors\n", r.name)
	} else if r.data.skipped {
		fmt.Printf("module %s finished (skipped)\n", r.name)
	} else {
		fmt.Printf("module %s finished\n", r.name)
	}
}

type Checker struct {
	fs       *scaleadpt.FileSystem
	spec     *SpecFilesystem
	reporter *Reporter
}

func CheckFS(spec *SpecFilesystem) bool {
	c := Checker{
		fs:       scaleadpt.OpenFileSystem(spec.Name),
		spec:     spec,
		reporter: &Reporter{},
	}

	c.checkMountRoot()
	c.checkVersion()
	c.checkSnapshotDirsInfo()
	c.checkCreateGetDeleteSnapshot()
	c.checkListPolicy()

	c.reporter.PrintSummary()

	return c.reporter.IsSuccessful()
}

func (c *Checker) checkMountRoot() {
	f := c.reporter.StartModule("mount_root")
	defer f.Finish()

	if c.spec.MountRoot == "" {
		f.ReportSkipped()
		return
	}

	mountRoot, err := c.fs.GetMountRoot()
	if err != nil {
		f.AddRuntimeErr(err)
		return
	}

	f.AddMismatchErr(
		ShouldEqualString(mountRoot, c.spec.MountRoot, "mount_root"),
	)
}

func (c *Checker) checkVersion() {
	f := c.reporter.StartModule("version")
	defer f.Finish()

	if c.spec.Version == "" {
		f.ReportSkipped()
		return
	}

	version, err := c.fs.GetVersion()
	if err != nil {
		f.AddRuntimeErr(err)
		return
	}

	f.AddMismatchErr(
		ShouldEqualString(version, c.spec.Version, "version"),
	)
}

func (c *Checker) checkSnapshotDirsInfo() {
	f := c.reporter.StartModule("snapshot_dirs_info")
	defer f.Finish()

	if c.spec.SnapshotDirsInfo.Global == "" || c.spec.SnapshotDirsInfo.Fileset == "" {
		f.ReportSkipped()
		return
	}

	info, err := c.fs.GetSnapshotDirsInfo()
	if err != nil {
		f.AddRuntimeErr(err)
		return
	}

	f.AddMismatchErr(
		ShouldEqualString(info.Global, c.spec.SnapshotDirsInfo.Global, "global"),
		ShouldEqualBool(info.GlobalsInFileset, c.spec.SnapshotDirsInfo.GlobalsInFileset, "globals_in_fileset"),
		ShouldEqualString(info.Fileset, c.spec.SnapshotDirsInfo.Fileset, "fileset"),
		ShouldEqualBool(info.AllDirectories, c.spec.SnapshotDirsInfo.AllDirectories, "all_directories"),
	)
}

func (c *Checker) checkCreateGetDeleteSnapshot() {
	f := c.reporter.StartModule("create_get_delete_snapshot")
	defer f.Finish()

	if c.spec.CreateGetDeleteSnapshot == "" {
		f.ReportSkipped()
		return
	}

	start := time.Now()

	sCreate, err := c.fs.CreateSnapshot(c.spec.CreateGetDeleteSnapshot)
	if err != nil {
		f.AddRuntimeErr(err)
		return
	}

	end := time.Now()

	dur := end.Sub(start)
	mid := start.Add(dur / 2)
	intv := dur/2 + 10*time.Second

	f.AddMismatchErr(
		ShouldEqualString(sCreate.Name, c.spec.CreateGetDeleteSnapshot, "snapshot name"),
		ShouldEqualString(sCreate.Status, "Valid", "snapshot valid (dynamic)"),
		ShouldApproxEqualTime(sCreate.CreatedAt, mid, intv, "snapshot created_at (dynamic)"),
	)

	sGet, err := c.fs.GetSnapshot(sCreate.Name)
	if err != nil {
		f.AddRuntimeErr(err)
		_ = c.fs.DeleteSnapshot(sCreate.Name)
		return
	}

	f.AddMismatchErr(
		ShouldEqualInt(sGet.ID, sCreate.ID, "snapshot ID (dynamic)"),
		ShouldEqualString(sGet.Name, sCreate.Name, "snapshot name"),
		ShouldEqualString(sGet.Status, "Valid", "snapshot valid (dynamic)"),
		ShouldApproxEqualTime(sGet.CreatedAt, sCreate.CreatedAt, 10*time.Second, "snapshot created_at (dynamic)"),
		ShouldEqualString(sGet.Fileset, sCreate.Fileset, "snapshot fileset (dynamic)"),
	)

	err = c.fs.DeleteSnapshot(sCreate.Name)
	if err != nil {
		f.AddRuntimeErr(err)
		return
	}

	_, err = c.fs.GetSnapshot(sCreate.Name)
	if err == nil {
		f.AddRuntimeErr(fmt.Errorf("Snapshot \"%s\" still exists after deleting", sCreate.Name))
		return
	} else if err == scaleadpt.ErrSnapshotDoesNotExist {
	} else {
		f.AddRuntimeErr(err)
		return
	}
}

func (c *Checker) checkListPolicy() {
	f := c.reporter.StartModule("list_policy")
	defer f.Finish()

	if c.spec.ListPolicy.Subpath == "" {
		f.ReportSkipped()
		return
	}

	mountRoot, err := c.fs.GetMountRoot()
	if err != nil {
		f.AddRuntimeErr(err)
		return
	}

	basePath, err := filepath.Abs(filepath.Join(mountRoot, c.spec.ListPolicy.Subpath))
	if err != nil {
		f.AddRuntimeErr(err)
		return
	}

	specFiles := make(map[string]*SpecListPolicyFile)
	for i := range c.spec.ListPolicy.Files {
		file := &c.spec.ListPolicy.Files[i]
		specFiles[file.Path] = file
	}

	parser, err := filelist.ApplyPolicy(c.fs, scaleadpt.PolicyOpt.Subpath(c.spec.ListPolicy.Subpath))
	if err != nil {
		f.AddRuntimeErr(err)
		return
	}
	defer parser.Close()

	for {
		var fileData filelist.FileData
		err := parser.ParseLine(&fileData)
		if err == io.EOF {
			break
		} else if err != nil {
			f.AddRuntimeErr(err)
			return
		}

		relPath, err := filepath.Rel(basePath, fileData.Path)
		if err != nil {
			f.AddRuntimeErr(err)
			return
		}

		if specFile, ok := specFiles[relPath]; ok {
			if specFile == nil {
				f.AddRuntimeErr(
					fmt.Errorf("Encountered file %s a second time in file list", relPath),
				)
				continue
			}

			f.AddMismatchErr(
				ShouldEqualInt(int(fileData.FileSize), int(specFile.Size), "file size"),
			)

			specFiles[relPath] = nil
		} else if c.spec.ListPolicy.FilesComplete {
			f.AddMismatchErr(
				fmt.Errorf("Encountered file %s in file list which is not in spec", relPath),
			)
		}
	}

	for _, specFile := range specFiles {
		if specFile != nil {
			f.AddMismatchErr(
				fmt.Errorf("File %s from spec not encountered in file list", specFile.Path),
			)
		}
	}
}

func CheckAll(spec *Spec) bool {
	successful := true

	for name, fsSpec := range spec.Filesystems {
		if fsSpec.Name == "" {
			fsSpec.Name = name
		}

		if !CheckFS(&fsSpec) {
			successful = false
		}
	}

	return successful
}

func readSpec(path string) (*Spec, error) {
	specFile, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer specFile.Close()

	spec := &Spec{}

	dec := yaml.NewDecoder(specFile)
	err = dec.Decode(spec)
	if err != nil {
		return nil, err
	}

	return spec, nil
}

func main() {
	spec, err := readSpec(os.Args[1])
	if err != nil {
		panic(err)
	}

	if !CheckAll(spec) {
		os.Exit(1)
	}
}
