package scaleadpt

// Driver is the interface providing access to low-level Spectrum Scale
// operations.
//
// For simplicity, the Driver interface re-uses the high-level types of the
// scaleadpt package. Drivers will not propagate all fields, i.e. fields used
// only used as part of the high-level api are not read or written.
type Driver interface {
	CreateSnapshot(filesystem string, name string, options *snapshotOptions) error
	GetSnapshot(filesystem string, name string) (*Snapshot, error)
	DeleteSnapshot(filesystem string, name string) error

	ApplyPolicy(filesystem string, policy *Policy, options *policyOptions) error
}
