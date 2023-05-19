package options

// FilelistPolicyOptions is the option aggregate structure for options for
// policy apply operations in the filelist package.
type FilelistPolicyOptions struct {
	PolicyOptions       PolicyOptions
	ExcludePathPatterns []string
}

func (f *FilelistPolicyOptions) Apply(opts []FilelistPolicyOptioner) *FilelistPolicyOptions {
	for _, opt := range opts {
		opt.apply(f)
	}

	return f
}

func (f *FilelistPolicyOptions) apply(options *FilelistPolicyOptions) {
	*options = *f
}

type FilelistPolicyOptioner interface {
	apply(*FilelistPolicyOptions)
}

type filelistPolicyOptionerFunc func(*FilelistPolicyOptions)

func (f filelistPolicyOptionerFunc) apply(options *FilelistPolicyOptions) {
	f(options)
}

type FilelistPolicyOptioners struct{}

func (_ FilelistPolicyOptioners) PolicyOpts(opts ...PolicyOptioner) FilelistPolicyOptioner {
	return filelistPolicyOptionerFunc(func(options *FilelistPolicyOptions) {
		options.PolicyOptions = PolicyOptions{}
		options.PolicyOptions.Apply(opts)
	})
}

func (_ FilelistPolicyOptioners) ExcludePathPatterns(patterns []string) FilelistPolicyOptioner {
	return filelistPolicyOptionerFunc(func(options *FilelistPolicyOptions) {
		options.ExcludePathPatterns = patterns
	})
}
