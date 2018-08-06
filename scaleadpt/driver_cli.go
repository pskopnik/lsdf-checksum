package scaleadpt

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
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

	"github.com/akutz/gofsutil"

	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt/internal/options"
	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt/internal/utils"
)

var (
	SnapshotDoesNotExist = errors.New("Snapshot does not exist.")
	UnexpectedFormat     = errors.New("Unexpected format encountered.")
	PrefixNotFound       = errors.New("Prefix not found.")
	HeaderNotFound       = errors.New("A header field was not found.")
	FileSystemNotFound   = errors.New("The file system was not found.")
)

const gpfsMountType = "gpfs"

// gpfsEntryScan is an EntryScanFunc for gofsutil.
// The function should be used to scan all mounted gpfs based file systems.
// The implementation is an adapted copy of the gofsutil's
// defaultEntryScanFunc, see:
//
//    https://github.com/akutz/gofsutil/blob/master/gofsutil_mount.go#L99
func gpfsEntryScan(ctx context.Context, entry gofsutil.Entry, cache map[string]gofsutil.Entry) (gofsutil.Info, bool, error) {
	var info gofsutil.Info

	if entry.FSType != gpfsMountType {
		return info, false, nil
	}

	// Copy the Entry object's fields to the Info object.
	info.Device = entry.MountSource
	info.Opts = make([]string, len(entry.MountOpts))
	copy(info.Opts, entry.MountOpts)
	info.Path = entry.MountPoint
	info.Type = entry.FSType
	info.Source = entry.MountSource

	if cachedEntry, ok := cache[entry.MountSource]; ok {
		info.Source = filepath.Join(cachedEntry.MountPoint, entry.Root)
	} else {
		cache[entry.MountSource] = entry
	}

	return info, true, nil
}

var gpfsFsInfo = &gofsutil.FS{
	ScanEntry: gpfsEntryScan,
}

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

var (
	gpfsErrorRegExp = regexp.MustCompile("^GPFS: (?P<messagecode>(?P<first>\\d+)-(?P<second>\\d+))( \\[(?P<severity>.)(:(?P<errcode>\\d+))?\\])?")

	snapdirOneLineRegExp = regexp.MustCompile(
		`^Snapshot directory for "([^"]+)" is "([^"]+)" \(((root directory only)|(all directories))\)
$`)
	snapdirTwoLineRegExp = regexp.MustCompile(
		`^Fileset snapshot directory for "([^"]+)" is "([^"]+)" \(((root directory only)|(all directories))\)
Global snapshot directory for "([^"]+)" is "([^"]+)" in ((root fileset)|(all filesets))
$`)
)

var _ Driver = &CLIDriver{}

type CLIDriver struct {
}

func (c *CLIDriver) CreateSnapshot(filesystem DriverFileSystem, name string, opts *options.SnapshotOptions) error {
	args := []string{
		filesystem.GetName(),
		name,
	}

	if len(opts.Fileset) > 0 {
		args = append(args, "-j", opts.Fileset)
	}

	_, err := c.runOutput("mmcrsnapshot", cmdArgs(args...))

	return err
}

func (c *CLIDriver) GetSnapshot(filesystem DriverFileSystem, name string) (*Snapshot, error) {
	output, err := c.runOutput("mmlssnapshot", cmdArgs(filesystem.GetName(), "-s", name, "-Y"))
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
	reader := newGpfsCmdOutReader(
		bytes.NewReader(output),
		"mmlssnapshot",
		[]string{
			"directory",
			"snapID",
			"status",
			"created",
			"fileset",
		},
	)

	const (
		directoryField int = iota
		snapIDField
		statusField
		createdField
		filesetField
	)

	snapshots := make([]*Snapshot, 0)

	for reader.Scan() {
		row := reader.Row()

		id, err := strconv.ParseInt(row[snapIDField], 10, 32)
		if err != nil {
			return nil, err
		}

		createdAt, err := time.ParseInLocation(
			time.ANSIC,
			row[createdField],
			time.Local,
		)
		if err != nil {
			return nil, err
		}

		snapshots = append(
			snapshots,
			&Snapshot{
				Id:        int(id),
				Name:      row[directoryField],
				Status:    row[statusField],
				CreatedAt: createdAt,
				Fileset:   row[filesetField],
			},
		)
	}
	if err := reader.Err(); err != nil {
		return nil, err
	}

	return snapshots, nil
}

func (c *CLIDriver) DeleteSnapshot(filesystem DriverFileSystem, name string) error {
	_, err := c.runOutput("mmdelsnapshot", cmdArgs(filesystem.GetName(), name))
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

func (c *CLIDriver) ApplyPolicy(filesystem DriverFileSystem, policy *Policy, opts *options.PolicyOptions) error {
	args := []string{}

	if len(opts.Subpath) > 0 {
		rootDir, err := c.GetMountRoot(filesystem)
		if err != nil {
			return err
		}
		snapshotDirsInfo, err := c.GetSnapshotDirsInfo(filesystem)
		if err != nil {
			return err
		}

		if len(opts.SnapshotName) > 0 {
			rootDir = filepath.Join(rootDir, snapshotDirsInfo.Global, opts.SnapshotName)
		}

		args = append(args, filepath.Join(rootDir, opts.Subpath))
	} else {
		args = append(args, filesystem.GetName())
	}

	policyFilePath, err := c.writePolicyFile(policy)
	if err != nil {
		return err
	}
	defer os.Remove(policyFilePath)

	args = append(args, "-P", policyFilePath)

	if len(opts.SnapshotName) > 0 {
		args = append(args, "-S", opts.SnapshotName)
	}
	if len(opts.Action) > 0 {
		args = append(args, "-I", opts.Action)
	}
	if len(opts.FileListPrefix) > 0 {
		args = append(args, "-f", opts.FileListPrefix)
	}
	if len(opts.QoSClass) > 0 {
		args = append(args, "--qos", opts.QoSClass)
	}
	if len(opts.NodeList) > 0 {
		args = append(args, "-N", strings.Join(opts.NodeList, ","))
	}
	if len(opts.GlobalWorkDirectory) > 0 {
		args = append(args, "-g", opts.GlobalWorkDirectory)
	}
	for key, val := range opts.Substitutions {
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

func (c *CLIDriver) GetMountRoot(filesystem DriverFileSystem) (string, error) {
	fileSystemRoot, err := c.getMountRootLsfs(filesystem)
	if err == nil {
		return fileSystemRoot, nil
	} else if !(utils.IsExecNotFound(err) || err == PrefixNotFound) {
		return "", err
	}

	fileSystemRoot, err = c.getMountRootMount(filesystem)
	if err != nil {
		return "", err
	}
	return fileSystemRoot, nil
}

func (c *CLIDriver) getMountRootLsfs(filesystem DriverFileSystem) (string, error) {
	filesystemName := filesystem.GetName()

	output, err := c.runOutput("lsfs", cmdArgs("-v", "-Y"))
	if err != nil {
		return "", err
	}

	reader := newGpfsCmdOutReader(
		bytes.NewReader(output),
		"lsfs",
		[]string{
			"Device name",
			"Mount point",
		},
	)

	const (
		deviceNameField int = iota
		mountPointField
	)

	for reader.Scan() {
		row := reader.Row()

		if row[deviceNameField] == filesystemName {
			return row[mountPointField], nil
		}
	}
	if err = reader.Err(); err != nil {
		return "", err
	}

	return "", FileSystemNotFound
}

func (c *CLIDriver) getMountRootMount(filesystem DriverFileSystem) (string, error) {
	filesystemName := filesystem.GetName()

	mounts, err := gpfsFsInfo.GetMounts(context.Background())
	if err != nil {
		return "", err
	}

	for _, mount := range mounts {
		if mount.Source == filesystemName {
			return mount.Path, nil
		}
	}

	return "", FileSystemNotFound
}

func (c *CLIDriver) GetSnapshotDirsInfo(filesystem DriverFileSystem) (*SnapshotDirsInfo, error) {
	filesystemName := filesystem.GetName()

	output, err := c.runOutput("mmsnapdir", cmdArgs(filesystemName, "-q"))
	if err != nil {
		// Possible errors:
		// mmsnapdir: 6027-1388 File system ... is not known to the GPFS cluster.
		// This is not supported by wrapError, as the message does not start with GPFS:
		return nil, err
	}

	return c.parseSnapdirOutput(output, filesystemName)
}

func (c *CLIDriver) parseSnapdirOutput(output []byte, filesystemName string) (*SnapshotDirsInfo, error) {
	lineCount := bytes.Count(output, []byte("\n"))
	if lineCount == 1 {
		submatches := snapdirOneLineRegExp.FindSubmatch(output)
		if len(submatches) == 0 {
			return nil, UnexpectedFormat
		}

		if string(submatches[1]) != filesystemName {
			return nil, UnexpectedFormat
		}

		snapshotDirsInfo := &SnapshotDirsInfo{}

		snapshotDirsInfo.Fileset = string(submatches[2])

		if len(submatches[4]) > 0 {
			snapshotDirsInfo.AllDirectories = false
		} else if len(submatches[5]) > 0 {
			snapshotDirsInfo.AllDirectories = true
		} else {
			return nil, UnexpectedFormat
		}

		snapshotDirsInfo.Global = snapshotDirsInfo.Fileset

		snapshotDirsInfo.GlobalsInFileset = false

		return snapshotDirsInfo, nil
	} else if lineCount == 2 {
		submatches := snapdirTwoLineRegExp.FindSubmatch(output)
		if len(submatches) == 0 {
			return nil, UnexpectedFormat
		}

		if string(submatches[1]) != filesystemName || string(submatches[6]) != filesystemName {
			return nil, UnexpectedFormat
		}
		snapshotDirsInfo := &SnapshotDirsInfo{}

		snapshotDirsInfo.Fileset = string(submatches[2])

		if len(submatches[4]) > 0 {
			snapshotDirsInfo.AllDirectories = false
		} else if len(submatches[5]) > 0 {
			snapshotDirsInfo.AllDirectories = true
		} else {
			return nil, UnexpectedFormat
		}

		snapshotDirsInfo.Global = string(submatches[7])

		if len(submatches[9]) > 0 {
			snapshotDirsInfo.GlobalsInFileset = false
		} else if len(submatches[10]) > 0 {
			snapshotDirsInfo.GlobalsInFileset = true
		} else {
			return nil, UnexpectedFormat
		}

		return snapshotDirsInfo, nil
	} else {
		return nil, UnexpectedFormat
	}
}

func (c *CLIDriver) GetVersion(filesystem DriverFileSystem) (string, error) {
	filesystemName := filesystem.GetName()

	output, err := c.runOutput("mmlsfs", cmdArgs(filesystemName, "-Y", "-V"))
	if err != nil {
		// Possible errors:
		// mmlsfs: 6027-1388 File system ... is not known to the GPFS cluster.
		// This is not supported by wrapError, as the message does not start with GPFS:
		return "", err
	}

	reader := newGpfsCmdOutReader(
		bytes.NewReader(output),
		"mmlsfs",
		[]string{
			"deviceName",
			"fieldName",
			"data",
		},
	)

	const (
		deviceNameField int = iota
		fieldNameField
		dataField
	)

	for reader.Scan() {
		row := reader.Row()

		if row[deviceNameField] == filesystemName && row[fieldNameField] == "filesystemVersion" {
			return row[dataField], nil
		}
	}
	if err = reader.Err(); err != nil {
		return "", err
	}

	return "", UnexpectedFormat
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

type gpfsCmdOutReader struct {
	scanner *bufio.Scanner
	prefix  string
	headers []string
	indices []int

	row []string
	err error
}

func newGpfsCmdOutReader(r io.Reader, prefix string, headers []string) *gpfsCmdOutReader {
	return &gpfsCmdOutReader{
		prefix:  prefix,
		scanner: bufio.NewScanner(r),
		headers: headers,
	}
}

func (g *gpfsCmdOutReader) readHeader() error {
	if g.scanner.Scan() {
		fields := strings.Split(g.scanner.Text(), ":")

		if fields[0] != g.prefix {
			return PrefixNotFound
		}

		g.indices = make([]int, len(g.headers))
		headerFound := make([]bool, len(g.headers))

		// Iterate over all fields and store indices for headers which have
		// been requested.
		for fieldInd, field := range fields {
			for headerInd, header := range g.headers {
				if field == header {
					g.indices[headerInd] = fieldInd
					headerFound[headerInd] = true
				}
			}
		}

		// Check that all headers have been found
		for _, found := range headerFound {
			if !found {
				return HeaderNotFound
			}
		}
	} else if err := g.scanner.Err(); err != nil {
		return err
	} else {
		return UnexpectedFormat
	}

	return nil
}

// ReadRow reads and returns the next row of the reader.
// This is a low level method.
// You might want to check out the Scan() method.
func (g *gpfsCmdOutReader) ReadRow() ([]string, error) {
	var err error

	if g.indices == nil {
		err = g.readHeader()
		if err != nil {
			return nil, err
		}
	}

	if g.scanner.Scan() {
		fields := strings.Split(g.scanner.Text(), ":")

		values := make([]string, len(g.headers))
		for ind, fieldIndex := range g.indices {
			values[ind] = g.decodeField(fields[fieldIndex])
		}

		return values, nil
	} else if err = g.scanner.Err(); err != nil {
		return nil, err
	} else {
		return nil, io.EOF
	}
}

// Scan reads the next row from the reader and returns whether the reading was
// successful.
// If the reading was successful, Row() and Field() can be used to retrieve the
// row's content.
// If the reading was not successful, Err() returns the error. If the end of
// the file was reached, Scan() returns false and Err() returns nil.
//
// Scan allows simplified access similar to bufio.Scanner.
//
//    for reader.Scan() {
//    	row := reader.Row()
//
//    	// do something with row
//    }
//    if err = reader.Err(); err != nil {
//    	return nil, err
//    }
func (g *gpfsCmdOutReader) Scan() bool {
	row, err := g.ReadRow()
	if err != nil {
		if err != io.EOF {
			g.err = err
		}

		g.row = nil

		return false
	}

	g.row = row
	g.err = nil

	return true
}

// Row returns the last read row of the reader.
// This method must only be called after a successful call to Scan().
func (g *gpfsCmdOutReader) Row() []string {
	return g.row
}

// Field returns a field from the last read row of the reader.
// This method must only be called after a successful call to Scan().
func (g *gpfsCmdOutReader) Field(index int) string {
	return g.Field(index)
}

// Err returns the error yielded by the last read performed by Scan().
func (g *gpfsCmdOutReader) Err() error {
	return g.err
}

func (_ *gpfsCmdOutReader) decodeField(s string) string {
	decoded, err := url.QueryUnescape(s)
	if err != nil {
		return ""
	}

	return decoded
}
