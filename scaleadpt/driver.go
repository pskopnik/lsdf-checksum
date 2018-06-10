package scaleadpt

// DriverFileSystem is the interface expected by Drivers' providing
// information about the FileSystem.
type DriverFileSystem interface {
	GetName() string
}

// Driver is the interface providing access to low-level Spectrum Scale
// operations.
//
// For simplicity, the Driver interface re-uses the high-level types of the
// scaleadpt package. Drivers will not propagate all fields, i.e. fields used
// only used as part of the high-level api are not read or written.
type Driver interface {
	GetVersion(filesystem DriverFileSystem) (string, error)

	CreateSnapshot(filesystem DriverFileSystem, name string, options *snapshotOptions) error
	GetSnapshot(filesystem DriverFileSystem, name string) (*Snapshot, error)
	DeleteSnapshot(filesystem DriverFileSystem, name string) error

	ApplyPolicy(filesystem DriverFileSystem, policy *Policy, options *policyOptions) error

	GetMountRoot(filesystem DriverFileSystem) (string, error)
	GetSnapshotDirsInfo(filesystem DriverFileSystem) (*SnapshotDirsInfo, error)
}
