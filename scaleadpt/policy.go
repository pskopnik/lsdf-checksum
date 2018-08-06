package scaleadpt

import (
	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt/internal/options"
)

// PolicyOpt provides simple access to PolicyOptioners methods.
var PolicyOpt = options.PolicyOptioners{}

type Policy struct {
	Rules []*Rule
}

type Rule struct {
	RuleName string
	Content  string
}
