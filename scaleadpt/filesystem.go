package scaleadpt

import (
	"strings"

	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt/internal/utils"
	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt/options"
)

var _ DriverFileSystem = &FileSystem{}

type FileSystem struct {
	name   string
	driver Driver
}

func OpenFileSystem(name string) *FileSystem {
	return OpenFileSystemWithDriver(name, &CLIDriver{})
}

func OpenFileSystemWithDriver(name string, driver Driver) *FileSystem {
	return &FileSystem{
		name:   name,
		driver: driver,
	}
}

func (f *FileSystem) GetName() string {
	return f.name
}

func (f *FileSystem) GetVersion() (string, error) {
	return f.driver.GetVersion(f)
}

func (f *FileSystem) CreateSnapshot(name string, opts ...options.SnapshotOptioner) (*Snapshot, error) {
	optionsAggregate := (&options.SnapshotOptions{}).Apply(opts)

	err := f.driver.CreateSnapshot(f, name, optionsAggregate)
	if err != nil {
		return nil, err
	}

	snapshot, err := f.driver.GetSnapshot(f, name)
	if err != nil {
		_ = f.driver.DeleteSnapshot(f, name)
		return nil, err
	}

	snapshot.filesystem = f
	return snapshot, err
}

func (f *FileSystem) GetSnapshot(name string) (*Snapshot, error) {
	snapshot, err := f.driver.GetSnapshot(f, name)
	if err != nil {
		return nil, err
	}

	snapshot.filesystem = f
	return snapshot, nil
}

func (f *FileSystem) DeleteSnapshot(name string) error {
	return f.driver.DeleteSnapshot(f, name)
}

func (f *FileSystem) ApplyPolicy(policy *Policy, opts ...options.PolicyOptioner) error {
	optionsAggregate := (&options.PolicyOptions{}).Apply(opts)

	return f.driver.ApplyPolicy(f, policy, optionsAggregate)
}

func (f *FileSystem) ApplyListPolicy(policy *Policy, listPath string, opts ...options.PolicyOptioner) error {
	var tempListPath, fileListPrefix string
	var requiresMove bool

	// Extract TempDir from opts
	tempDir := (&options.PolicyOptions{}).Apply(opts).TempDir

	if strings.HasSuffix(listPath, ".list.files") {
		// If the listPath end with ".list.files" it is usable directly as the
		// path for the FileListPrefix option
		tempListPath = listPath
	} else {
		requiresMove = true
		tempListPath = utils.TouchNonExistingTempFile(
			"scaleadpt-applylistpolicy-",
			".list.files",
			tempDir,
		)
	}
	fileListPrefix = tempListPath[0 : len(tempListPath)-len(".list.files")]

	opts = append(
		opts,
		PolicyOpt.Action("defer"),
		PolicyOpt.FileListPrefix(fileListPrefix),
	)

	err := f.ApplyPolicy(policy, opts...)
	if err != nil {
		return err
	}

	if requiresMove {
		return utils.MoveFile(fileListPrefix+".list.files", listPath)
	} else {
		return nil
	}
}

func (f *FileSystem) GetMountRoot() (string, error) {
	return f.driver.GetMountRoot(f)
}

func (f *FileSystem) GetSnapshotDirsInfo() (*SnapshotDirsInfo, error) {
	return f.driver.GetSnapshotDirsInfo(f)
}
