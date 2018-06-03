package scaleadpt

type PolicyOptioner interface {
	apply(*policyOptions)
}

type policyOptionerFunc func(*policyOptions)

func (p policyOptionerFunc) apply(options *policyOptions) {
	p(options)
}

type PolicyOptions struct{}

func (_ PolicyOptions) Subpath(subpath string) PolicyOptioner {
	return policyOptionerFunc(func(options *policyOptions) {
		options.Subpath = subpath
	})
}

func (_ PolicyOptions) SnapshotName(snapshotName string) PolicyOptioner {
	return policyOptionerFunc(func(options *policyOptions) {
		options.SnapshotName = snapshotName
	})
}

func (_ PolicyOptions) Action(action string) PolicyOptioner {
	return policyOptionerFunc(func(options *policyOptions) {
		options.Action = action
	})
}

func (_ PolicyOptions) FileListPrefix(fileListPrefix string) PolicyOptioner {
	return policyOptionerFunc(func(options *policyOptions) {
		options.FileListPrefix = fileListPrefix
	})
}

func (_ PolicyOptions) QoSClass(qosClass string) PolicyOptioner {
	return policyOptionerFunc(func(options *policyOptions) {
		options.QoSClass = qosClass
	})
}

func (_ PolicyOptions) NodeList(nodeList []string) PolicyOptioner {
	return policyOptionerFunc(func(options *policyOptions) {
		options.NodeList = nodeList
	})
}

func (_ PolicyOptions) AddToNodeList(node string) PolicyOptioner {
	return policyOptionerFunc(func(options *policyOptions) {
		options.NodeList = append(options.NodeList, node)
	})
}

func (_ PolicyOptions) GlobalWorkDirectory(globalWorkDirectory string) PolicyOptioner {
	return policyOptionerFunc(func(options *policyOptions) {
		options.GlobalWorkDirectory = globalWorkDirectory
	})
}

func (_ PolicyOptions) Substitutions(substitutions map[string]string) PolicyOptioner {
	return policyOptionerFunc(func(options *policyOptions) {
		options.Substitutions = substitutions
	})
}

func (_ PolicyOptions) AddSubstitution(key string, value string) PolicyOptioner {
	return policyOptionerFunc(func(options *policyOptions) {
		options.Substitutions[key] = value
	})
}

// PolicyOpt provides simple access to PolicyOptions methods.
var PolicyOpt = PolicyOptions{}

// policyOptions is the option aggregate structure for options for policy
// apply operations.
type policyOptions struct {
	Subpath             string
	SnapshotName        string
	Action              string
	FileListPrefix      string
	QoSClass            string
	NodeList            []string
	GlobalWorkDirectory string
	Substitutions       map[string]string
}

type Policy struct {
	Rules []*Rule
}

type Rule struct {
	RuleName string
	Content  string
}
