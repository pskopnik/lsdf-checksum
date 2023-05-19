package options

// PolicyOptions is the option aggregate structure for options for policy
// apply operations.
type PolicyOptions struct {
	Subpath             string
	SnapshotName        string
	Action              string
	FileListPrefix      string
	QoSClass            string
	NodeList            []string
	GlobalWorkDirectory string
	Substitutions       map[string]string
	// TempDir is the temporary directory used by functions in this package.
	// It is also passed as the LocalWorkDirectory to policy apply operations.
	// If this is empty (i.e. not explicitly set) `/tmp` is in all likelihood
	// be used.
	// The directory must exist and be writable.
	TempDir string
}

func (p *PolicyOptions) Apply(opts []PolicyOptioner) *PolicyOptions {
	for _, opt := range opts {
		opt.apply(p)
	}

	return p
}

func (p *PolicyOptions) apply(options *PolicyOptions) {
	*options = *p
}

type PolicyOptioner interface {
	apply(*PolicyOptions)
}

type policyOptionerFunc func(*PolicyOptions)

func (p policyOptionerFunc) apply(options *PolicyOptions) {
	p(options)
}

type PolicyOptioners struct{}

func (_ PolicyOptioners) Subpath(subpath string) PolicyOptioner {
	return policyOptionerFunc(func(options *PolicyOptions) {
		options.Subpath = subpath
	})
}

func (_ PolicyOptioners) SnapshotName(snapshotName string) PolicyOptioner {
	return policyOptionerFunc(func(options *PolicyOptions) {
		options.SnapshotName = snapshotName
	})
}

func (_ PolicyOptioners) Action(action string) PolicyOptioner {
	return policyOptionerFunc(func(options *PolicyOptions) {
		options.Action = action
	})
}

func (_ PolicyOptioners) FileListPrefix(fileListPrefix string) PolicyOptioner {
	return policyOptionerFunc(func(options *PolicyOptions) {
		options.FileListPrefix = fileListPrefix
	})
}

func (_ PolicyOptioners) QoSClass(qosClass string) PolicyOptioner {
	return policyOptionerFunc(func(options *PolicyOptions) {
		options.QoSClass = qosClass
	})
}

func (_ PolicyOptioners) NodeList(nodeList []string) PolicyOptioner {
	return policyOptionerFunc(func(options *PolicyOptions) {
		options.NodeList = nodeList
	})
}

func (_ PolicyOptioners) AddToNodeList(node string) PolicyOptioner {
	return policyOptionerFunc(func(options *PolicyOptions) {
		options.NodeList = append(options.NodeList, node)
	})
}

func (_ PolicyOptioners) GlobalWorkDirectory(globalWorkDirectory string) PolicyOptioner {
	return policyOptionerFunc(func(options *PolicyOptions) {
		options.GlobalWorkDirectory = globalWorkDirectory
	})
}

func (_ PolicyOptioners) Substitutions(substitutions map[string]string) PolicyOptioner {
	return policyOptionerFunc(func(options *PolicyOptions) {
		options.Substitutions = substitutions
	})
}

func (_ PolicyOptioners) AddSubstitution(key string, value string) PolicyOptioner {
	return policyOptionerFunc(func(options *PolicyOptions) {
		options.Substitutions[key] = value
	})
}

func (_ PolicyOptioners) TempDir(tempDir string) PolicyOptioner {
	return policyOptionerFunc(func(options *PolicyOptions) {
		options.TempDir = tempDir
	})
}
