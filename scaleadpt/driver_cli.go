package scaleadpt

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var (
	SnapshotDoesNotExist = errors.New("Snapshot does not exist.")
	UnexpectedFormat     = errors.New("Unexpected format encountered.")
)

var _ error = &CLIError{}

type CLIError struct {
	*exec.ExitError

	// ExitStatus it the exit status of the command.
	// If unavailable, ExitStatus is set to 0.
	ExitStatus int
	// Stderr is the stderr output of the command as a string.
	Stderr string
}

func (c *CLIError) Error() string {
	if c.ExitStatus > 0 {
		return fmt.Sprintf(
			"Command failed, exit status = %d.\nStderr of the command:\n%s",
			c.ExitStatus,
			c.Stderr,
		)
	} else {
		return fmt.Sprintf(
			"Command failed.\nStderr of the command:\n%s",
			c.Stderr,
		)
	}
}

var _ error = &CLIGPFSError{}

// CLIGPFSError is an error type containing GPFS specific information.
//
//   https://www.ibm.com/support/knowledgecenter/en/STXKQY_4.2.3/com.ibm.spectrum.scale.v4r23.doc/bl1pdg_message.htm
//   https://www.ibm.com/support/knowledgecenter/STXKQY_4.2.3/com.ibm.spectrum.scale.v4r23.doc/bl1pdg_messageseveritytags.htm
type CLIGPFSError struct {
	CLIError

	// MessageCode is the GPFS message code, such as "6027-2684"
	MessageCode string
	// Severity is the optional message severity.
	// This is one letter, such as "E".
	Severity string
	// ErrorCode is the optional error code included with the message.
	// See the GPFS documentation on Message severity tags for details.
	// If unavailable, ErrorCode is set to 0.
	ErrorCode int
}

type cmdOption interface {
	apply(*exec.Cmd)
}

type cmdOptionFunc func(*exec.Cmd)

func (c cmdOptionFunc) apply(cmd *exec.Cmd) {
	c(cmd)
}

func cmdArgs(args ...string) cmdOption {
	return cmdOptionFunc(func(cmd *exec.Cmd) {
		cmd.Args = append(cmd.Args, args...)
	})
}

var gpfsErrorRegExp = regexp.MustCompile("^GPFS: (?P<messagecode>(?P<first>\\d+)-(?P<second>\\d+))( \\[(?P<severity>.)(:(?P<errcode>\\d+))?\\])?")

var _ Driver = &CLIDriver{}

type CLIDriver struct {
}

func (c *CLIDriver) CreateSnapshot(filesystem string, name string, options *snapshotOptions) error {
	args := []string{
		filesystem,
		name,
	}

	if len(options.Fileset) > 0 {
		args = append(args, "-j", options.Fileset)
	}

	_, err := c.runOutput("mmcrsnapshot", cmdArgs(args...))

	return err
}

func (c *CLIDriver) GetSnapshot(filesystem string, name string) (*Snapshot, error) {
	output, err := c.runOutput("mmlssnapshot", cmdArgs(filesystem, "-s", name, "-Y"))
	if err != nil {
		if gpfsError, ok := err.(*CLIGPFSError); ok {
			if gpfsError.MessageCode == "6027-2612" {
				// https://www.ibm.com/support/knowledgecenter/STXKQY_4.2.3/com.ibm.spectrum.scale.v4r23.doc/6027-2612.htm
				return nil, SnapshotDoesNotExist
			}
		}

		return nil, err
	}

	snapshots, err := c.parseListSnapshots(output)
	if err != nil {
		return nil, err
	}

	if len(snapshots) != 1 {
		return nil, UnexpectedFormat
	}

	return snapshots[0], nil
}

func (c *CLIDriver) parseListSnapshots(output []byte) ([]*Snapshot, error) {
	scanner := bufio.NewScanner(bytes.NewReader(output))

	columnIndices := struct {
		directory int
		snapID    int
		status    int
		created   int
		fileset   int
	}{}

	if scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "No snapshots") {
			return nil, nil
		}

		// Read header and extract column indices for relevant fields.
		// The header is expected to look like this:
		// mmlssnapshot::HEADER:version:reserved:reserved:filesystemName:directory:snapID:status:created:quotas:data:metadata:fileset:snapType:

		fields := strings.Split(scanner.Text(), ":")
		for ind, name := range fields {
			switch name {
			case "directory":
				columnIndices.directory = ind
			case "snapID":
				columnIndices.snapID = ind
			case "status":
				columnIndices.status = ind
			case "created":
				columnIndices.created = ind
			case "fileset":
				columnIndices.fileset = ind
			}
		}
	} else {
		return nil, UnexpectedFormat
	}

	snapshots := make([]*Snapshot, 0)

	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), ":")

		id, err := strconv.ParseInt(c.clidecode(fields[columnIndices.snapID]), 10, 32)
		if err != nil {
			return nil, err
		}

		createdAt, err := time.ParseInLocation(
			time.ANSIC,
			c.clidecode(fields[columnIndices.created]),
			time.Local,
		)
		if err != nil {
			return nil, err
		}

		snapshots = append(
			snapshots,
			&Snapshot{
				Id:        int(id),
				Name:      c.clidecode(fields[columnIndices.directory]),
				Status:    c.clidecode(fields[columnIndices.status]),
				CreatedAt: createdAt,
				Fileset:   c.clidecode(fields[columnIndices.fileset]),
			},
		)
	}

	return snapshots, nil
}

func (c *CLIDriver) DeleteSnapshot(filesystem string, name string) error {
	_, err := c.runOutput("mmdelsnapshot", cmdArgs(filesystem, name))
	if err != nil {
		if gpfsError, ok := err.(*CLIGPFSError); ok {
			if gpfsError.MessageCode == "6027-2683" || gpfsError.MessageCode == "6027-2684" {
				// https://www.ibm.com/support/knowledgecenter/STXKQY_4.2.3/com.ibm.spectrum.scale.v4r23.doc/6027-2683.htm
				// https://www.ibm.com/support/knowledgecenter/STXKQY_4.2.3/com.ibm.spectrum.scale.v4r23.doc/6027-2684.htm
				return SnapshotDoesNotExist
			}
		}

		return err
	}

	return nil
}

func (c *CLIDriver) ApplyPolicy(filesystem string, policy *Policy, options *policyOptions) error {
	args := []string{}

	if len(options.Subpath) > 0 {
		args = append(args, filepath.Join("/", filesystem, options.Subpath))
	} else {
		args = append(args, filesystem)
	}

	policyFilePath, err := c.writePolicyFile(policy)
	if err != nil {
		return err
	}
	defer os.Remove(policyFilePath)

	args = append(args, "-P", policyFilePath)

	if len(options.SnapshotName) > 0 {
		args = append(args, "-S", options.SnapshotName)
	}
	if len(options.Action) > 0 {
		args = append(args, "-I", options.Action)
	}
	if len(options.FileListPrefix) > 0 {
		args = append(args, "-f", options.FileListPrefix)
	}
	if len(options.QoSClass) > 0 {
		args = append(args, "--qos", options.QoSClass)
	}
	if len(options.NodeList) > 0 {
		args = append(args, "-N", strings.Join(options.NodeList, ","))
	}
	if len(options.GlobalWorkDirectory) > 0 {
		args = append(args, "-g", options.GlobalWorkDirectory)
	}
	for key, val := range options.Substitutions {
		args = append(args, "-M", key+"="+val)
	}

	_, err = c.runOutput("mmapplypolicy", cmdArgs(args...))
	if err != nil {
		return err
	}

	return nil
}

func (c *CLIDriver) writePolicyFile(policy *Policy) (string, error) {
	rulesContent := make([]string, len(policy.Rules))

	for ind, rule := range policy.Rules {
		rulesContent[ind] = rule.Content
	}

	policyFileContent := strings.Join(rulesContent, "\n\n")

	f, err := ioutil.TempFile("", "scaleadpt-policy-file")
	if err != nil {
		return "", err
	}
	defer f.Close()

	_, err = f.WriteString(policyFileContent)
	if err != nil {
		return "", err
	}

	return f.Name(), nil
}

func (c *CLIDriver) clidecode(s string) string {
	decoded, err := url.QueryUnescape(s)
	if err != nil {
		return ""
	}

	return decoded
}

func (c *CLIDriver) runOutput(name string, options ...cmdOption) ([]byte, error) {
	cmd := exec.Command(name)

	for _, option := range options {
		option.apply(cmd)
	}

	output, err := cmd.Output()

	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			err = c.wrapError(exitErr)
		}

		return output, err
	}

	return output, nil
}

func (c *CLIDriver) wrapError(exitErr *exec.ExitError) error {
	stderr := string(exitErr.Stderr)

	exitStatus := 0
	if waitStatus, ok := exitErr.Sys().(syscall.WaitStatus); ok {
		exitStatus = waitStatus.ExitStatus()
	}

	submatches := gpfsErrorRegExp.FindStringSubmatch(stderr)

	cliErr := CLIError{
		ExitError:  exitErr,
		Stderr:     stderr,
		ExitStatus: exitStatus,
	}

	if len(submatches) > 0 {
		errorCode, _ := strconv.ParseInt(submatches[7], 10, 32)

		gpfsError := &CLIGPFSError{
			CLIError:    cliErr,
			MessageCode: submatches[1],
			Severity:    submatches[5],
			ErrorCode:   int(errorCode),
		}

		return gpfsError
	}

	return &cliErr
}

// func (c *CLIDriver) runNoOutput(name string, options ...cmdOption) error {
// 	// Would probably require an implementation of a prefixSuffixSaver to be safe.
// 	// See https://golang.org/src/os/exec/exec.go
// }
