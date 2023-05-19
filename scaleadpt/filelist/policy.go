package filelist

import (
	"fmt"
	"os"
	"strings"

	"git.scc.kit.edu/sdm/lsdf-checksum/internal/osutils"
	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt"
	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt/options"
)

// ESCAPE enables percent encoding based on RFC3986 and adds all characters
// after the initial % to the list of unreserved characters (not escaped).
// The goal is to include all common characters, to reduce unescaping work.
const escapeRuleContent = `RULE 'escapeRule'
	EXTERNAL LIST 'files'
	ESCAPE '%|/,:; @#'`
const listFilesRuleContent = `RULE 'listFilesRule'
	LIST 'files'
	SHOW('|' || varchar(file_size) || '|' || varchar(modification_time) || '|')`
const excludeRuleTemplate = `RULE 'excludeRule'
	LIST 'files'
	EXCLUDE WHERE {CLAUSES}
	ESCAPE '\'`

var baseRules []*scaleadpt.Rule = []*scaleadpt.Rule{
	{
		RuleName: "escapeRule",
		Content:  escapeRuleContent,
	},
	{
		RuleName: "listFilesRule",
		Content:  listFilesRuleContent,
	},
}

var FileListPolicy = &scaleadpt.Policy{
	Rules: []*scaleadpt.Rule{
		{
			RuleName: "escapeRule",
			Content:  escapeRuleContent,
		},
		{
			RuleName: "listFilesRule",
			Content:  listFilesRuleContent,
		},
	},
}

// CloseParser wraps a Parser and adds a Close semantic.
// Close should be called in all cases (after processing is finished or if an
// error occurs).
type CloseParser struct {
	Parser
	file *os.File
}

// Close closes open files and frees temporary resources.
func (p *CloseParser) Close() error {
	err := p.file.Close()
	rErr := os.Remove(p.file.Name())

	if err != nil {
		return err
	} else if rErr != nil {
		return rErr
	} else {
		return nil
	}
}

// Opt provides simple access to FilelistPolicyOptioners methods.
var Opt = options.FilelistPolicyOptioners{}

// ApplyPolicy applies the FileListPolicy and returns a CloseParser to read the
// matching files.
// The CloseParser should be closed in all cases (after processing is finished
// or if an error occurs).
func ApplyPolicy(fs *scaleadpt.FileSystem, opts ...options.FilelistPolicyOptioner) (*CloseParser, error) {
	optsAggregate := (&options.FilelistPolicyOptions{}).Apply(opts)

	// Extract TempDir from PolicyOptions
	tempDir := optsAggregate.PolicyOptions.TempDir

	listPath := osutils.TouchNonExistingTempFile("scaleadpt-filelist-", ".list.files", tempDir)

	policy := scaleadpt.Policy{
		Rules: append([]*scaleadpt.Rule{}, baseRules...),
	}

	if len(optsAggregate.ExcludePathPatterns) > 0 {
		excludeRule, err := createExcludePathPatternsRule(optsAggregate.ExcludePathPatterns)
		if err != nil {
			return nil, fmt.Errorf("ApplyPolicy: %w", err)
		}
		// Insert new exclude rule before the last rule (listFilesRule)
		rules := append([]*scaleadpt.Rule{}, policy.Rules[:len(policy.Rules)-1]...)
		rules = append(rules, excludeRule)
		rules = append(rules, policy.Rules[len(policy.Rules)-1])
		policy.Rules = rules
	}

	err := fs.ApplyListPolicy(&policy, listPath, &optsAggregate.PolicyOptions)
	if err != nil {
		return nil, fmt.Errorf("ApplyPolicy: %w", err)
	}

	f, err := os.Open(listPath)
	if err != nil {
		return nil, fmt.Errorf("ApplyPolicy: open filelist file: %w", err)
	}

	parser := NewParser(f)

	return &CloseParser{
		Parser: *parser,
		file:   f,
	}, nil
}

func createExcludePathPatternsRule(patterns []string) (*scaleadpt.Rule, error) {
	var builder strings.Builder

	for i, pattern := range patterns {
		if i > 0 {
			builder.WriteString("\n\tOR ")
		}
		builder.WriteString("path_name LIKE ")
		patternStr, err := patternToSQLString(pattern)
		if err != nil {
			return nil, fmt.Errorf("createExcludePathPatternsRule: %w", err)
		}
		builder.WriteString(patternStr)
	}

	ruleContent := strings.Replace(excludeRuleTemplate, "{CLAUSES}", builder.String(), 1)

	return &scaleadpt.Rule{
		RuleName: "excludeRule",
		Content:  ruleContent,
	}, nil
}

// patternToSQLString transforms an SQL pattern to an SQL string.
// Pattern means that '%' and '_' are valid characters which are not escaped.
// The output is a SQL string literal quoted with single quotes `'`.
// It is assumed that '\' has been configured as an escape character.
// Under this assumption '\%' and '\_' supplied by the user evaluate to the
// literal characters '%' and '_'.
func patternToSQLString(pattern string) (string, error) {
	var builder strings.Builder

	// A worst case approximation
	builder.Grow(2*len(pattern) + 2)

	builder.WriteRune('\'')
	for _, r := range pattern {
		// Originally written according to:
		//   https://dev.mysql.com/doc/refman/8.0/en/string-literals.html Table 9.1
		//   https://dev.mysql.com/doc/c-api/8.0/en/mysql-real-escape-string-quote.html
		// However, it seems that the GPFS policy engine does not support any
		// of the listed escape patterns.
		// TODO: Mostly that's fine. Only for `'` it is a problem. A workaround
		// may be to put individual single-quotes into double-quoted strings
		// and combine the two via the CONCAT() function.

		switch r {
		case '\'':
			return "", fmt.Errorf("patternToSQLString: single-quotes are not supported in "+
				"patterns, input: %s", pattern)
		//	builder.WriteRune('\\')
		//	builder.WriteRune('\'')
		default:
			builder.WriteRune(r)
		}
	}
	builder.WriteRune('\'')

	return builder.String(), nil
}
