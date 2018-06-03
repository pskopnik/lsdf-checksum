package scaleadpt

import (
	"strings"

	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt/internal"
)

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

func (f *FileSystem) CreateSnapshot(name string, options ...SnapshotOptioner) (*Snapshot, error) {
	optionsAggregate := snapshotOptions{}

	for _, option := range options {
		option.apply(&optionsAggregate)
	}

	err := f.driver.CreateSnapshot(f.name, name, &optionsAggregate)
	if err != nil {
		return nil, err
	}

	snapshot, err := f.driver.GetSnapshot(f.name, name)
	if err != nil {
		f.driver.DeleteSnapshot(f.name, name)
		return nil, err
	}

	snapshot.filesystem = f
	return snapshot, err
}

func (f *FileSystem) GetSnapshot(name string) (*Snapshot, error) {
	snapshot, err := f.driver.GetSnapshot(f.name, name)
	if err != nil {
		return nil, err
	}

	snapshot.filesystem = f
	return snapshot, nil
}

func (f *FileSystem) DeleteSnapshot(name string) error {
	return f.driver.DeleteSnapshot(f.name, name)
}

func (f *FileSystem) ApplyPolicy(policy *Policy, options ...PolicyOptioner) error {
	optionsAggregate := policyOptions{}

	for _, option := range options {
		option.apply(&optionsAggregate)
	}

	return f.driver.ApplyPolicy(f.name, policy, &optionsAggregate)
}

func (f *FileSystem) ApplyListPolicy(policy *Policy, listPath string, options ...PolicyOptioner) error {
	var tempListPath, fileListPrefix string
	var requiresMove bool

	if strings.HasSuffix(listPath, ".list.files") {
		// If the listPath end with ".list.files" it is directly usable as the
		// path for the FileListPrefix option
		tempListPath = listPath
	} else {
		requiresMove = true
		tempListPath = internal.NonExistingTempFile("scaleadpt-applylistpolicy-", ".list.files")
	}
	fileListPrefix = tempListPath[0 : len(tempListPath)-len(".list.files")]

	options = append(
		options,
		PolicyOpt.Action("defer"),
		PolicyOpt.FileListPrefix(fileListPrefix),
	)

	err := f.ApplyPolicy(policy, options...)
	if err != nil {
		return err
	}

	if requiresMove {
		return internal.MoveFile(fileListPrefix+".list.files", listPath)
	} else {
		return nil
	}
}
