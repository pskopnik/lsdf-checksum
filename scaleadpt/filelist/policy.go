package filelist

import (
	"bufio"
	"os"

	"git.scc.kit.edu/sdm/lsdf-checksum/internal/osutils"
	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt"
	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt/options"
)

const escapeRuleContent = `RULE 'escapeRule'
	EXTERNAL LIST 'files'
	ESCAPE '%|.,-: '`
const listFilesRuleContent = `RULE 'listFilesRule'
	LIST 'files'
	SHOW('|' || varchar(file_size) || '|' || varchar(modification_time) || '|')`

var FileListPolicy = &scaleadpt.Policy{
	Rules: []*scaleadpt.Rule{
		&scaleadpt.Rule{
			RuleName: "escapeRule",
			Content:  escapeRuleContent,
		},
		&scaleadpt.Rule{
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
func (a *CloseParser) Close() error {
	err := a.file.Close()
	rErr := os.Remove(a.file.Name())

	if err != nil {
		return err
	} else if rErr != nil {
		return rErr
	} else {
		return nil
	}
}

// ApplyPolicy applies the FileListPolicy and returns a CloseParser to read the
// matching files.
// The CloseParser should be closed in all cases (after processing is finished
// or if an error occurs).
func ApplyPolicy(fs *scaleadpt.FileSystem, opts ...options.PolicyOptioner) (*CloseParser, error) {
	// Extract TempDir from opts
	tempDir := (&options.PolicyOptions{}).Apply(opts).TempDir

	listPath := osutils.TouchNonExistingTempFile("scaleadpt-filelist-", ".list.files", tempDir)

	err := fs.ApplyListPolicy(FileListPolicy, listPath, opts...)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(listPath)
	if err != nil {
		return nil, err
	}

	bufferedF := bufio.NewReader(f)

	parser := NewParser(bufferedF)

	return &CloseParser{
		Parser: *parser,
		file:   f,
	}, nil
}
