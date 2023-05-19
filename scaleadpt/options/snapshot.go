package options

// SnapshotOptions is the option aggregate structure for options for snapshot
// create operations.
type SnapshotOptions struct {
	Fileset string
}

func (s *SnapshotOptions) Apply(opts []SnapshotOptioner) *SnapshotOptions {
	for _, opt := range opts {
		opt.apply(s)
	}

	return s
}

func (s *SnapshotOptions) apply(options *SnapshotOptions) {
	*options = *s
}

type SnapshotOptioner interface {
	apply(*SnapshotOptions)
}

type snapshotOptionerFunc func(*SnapshotOptions)

func (s snapshotOptionerFunc) apply(options *SnapshotOptions) {
	s(options)
}

type SnapshotOptioners struct{}

func (_ SnapshotOptioners) FileSet(fileset string) SnapshotOptioner {
	return snapshotOptionerFunc(func(options *SnapshotOptions) {
		options.Fileset = fileset
	})
}
