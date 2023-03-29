package main

import (
	cryptoRand "crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/exp/rand"

	"gopkg.in/yaml.v3"
)

type Config struct {
	RootDir  string        `yaml:"root_dir"`
	Seed     uint64        `yaml:"seed,omitempty"`
	Changers []ChangerSpec `yaml:"changers"`
}

type ChangerSpec struct {
	Append     *AppendSpec   `yaml:"append,omitempty"`
	Replace    *ReplaceSpec  `yaml:"replace,omitempty"`
	FlipByte   *FlipByteSpec `yaml:"flipbyte,omitempty"`
	Likelihood float64       `yaml:"likelihood"`
	ID         string        `yaml:"id,omitempty"`
	Corrupt    bool          `yaml:"corrupt,omitempty"`
}

type AppendSpec struct {
	Size int64 `yaml:"size"`
}

type ReplaceSpec struct {
	Size int64 `yaml:"size"`
}

type FlipByteSpec struct{}

type ChangeContext struct {
	Path        string
	Rand        *rand.Rand
	ContentRand *rand.Rand
	Info        os.FileInfo
}

type Changer interface {
	MethodName() string
	ChangeFile(ctx *ChangeContext) error
}

var _ Changer = &FlipByteChanger{}

type FlipByteChanger struct {
	spec *FlipByteSpec
}

func NewFlipByteChanger(spec *FlipByteSpec) *FlipByteChanger {
	return &FlipByteChanger{
		spec: spec,
	}
}

func (c *FlipByteChanger) MethodName() string {
	return "flipbyte"
}

func (c *FlipByteChanger) ChangeFile(ctx *ChangeContext) error {
	fileSize := ctx.Info.Size()
	if fileSize == 0 {
		return nil
	}

	f, err := os.OpenFile(ctx.Path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()

	off := ctx.ContentRand.Int63n(fileSize)
	var buf [1]byte
	_, err = f.ReadAt(buf[:], off)
	if err != nil {
		return err
	}

	buf[0] = ^buf[0]

	_, err = f.WriteAt(buf[:], off)
	if err != nil {
		return err
	}

	return nil
}

var _ Changer = &AppendChanger{}

type AppendChanger struct {
	spec *AppendSpec
}

func NewAppendChanger(spec *AppendSpec) *AppendChanger {
	return &AppendChanger{
		spec: spec,
	}
}

func (c *AppendChanger) MethodName() string {
	return "append"
}

func (c *AppendChanger) ChangeFile(ctx *ChangeContext) error {
	f, err := os.OpenFile(ctx.Path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.CopyN(f, ctx.ContentRand, c.spec.Size)
	if err != nil {
		return err
	}

	return nil
}

var _ Changer = &ReplaceChanger{}

type ReplaceChanger struct {
	spec *ReplaceSpec
}

func NewReplaceChanger(spec *ReplaceSpec) *ReplaceChanger {
	return &ReplaceChanger{
		spec: spec,
	}
}

func (c *ReplaceChanger) MethodName() string {
	return "replace"
}

func (c *ReplaceChanger) ChangeFile(ctx *ChangeContext) error {
	fileSize := ctx.Info.Size()
	if fileSize == 0 {
		return nil
	}

	f, err := os.OpenFile(ctx.Path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()

	off := ctx.ContentRand.Int63n(fileSize)
	f.Seek(off, io.SeekStart)

	bytesToWrite := c.spec.Size
	if bytesToWrite > fileSize-off {
		bytesToWrite = fileSize - off

		// Replace at least one byte
		if bytesToWrite == 0 {
			// fileSize > 0, so off >= 1
			f.Seek(off-1, io.SeekStart)
			bytesToWrite = 1
		}
	}

	_, err = io.CopyN(f, ctx.ContentRand, bytesToWrite)
	if err != nil {
		return err
	}

	return nil
}

type Reporter struct {
	encoder *json.Encoder

	filesUnchanged     int64
	filesChanged       int64
	filesCorrupted     int64
	filesCorruptedOnly int64
}

type Summary struct {
	FilesUnchanged     int64
	FilesChanged       int64
	FilesCorrupted     int64
	FilesCorruptedOnly int64
}

func (r *Reporter) Summarise() Summary {
	return Summary{
		FilesUnchanged:     r.filesUnchanged,
		FilesChanged:       r.filesChanged,
		FilesCorrupted:     r.filesCorrupted,
		FilesCorruptedOnly: r.filesCorruptedOnly,
	}
}

func (r *Reporter) StartFile(path string) *reporterFileCtx {
	return &reporterFileCtx{
		reporter: r,
		path:     path,
	}
}

type reporterFileCtx struct {
	reporter      *Reporter
	path          string
	changers      []changeLogChanger
	corrupted     bool
	corruptedOnly bool
}

func (r *reporterFileCtx) AddChanger(id string, changer Changer, corrupted bool) {
	r.changers = append(r.changers, changeLogChanger{
		ID:     id,
		Method: changer.MethodName(),
	})
	if r.corruptedOnly {
		if !corrupted {
			r.corruptedOnly = false
		}
	} else {
		if corrupted {
			if len(r.changers) == 1 {
				// This changer is the first
				r.corruptedOnly = true
			}
			r.corrupted = true
		}
	}
}

type changeLogEvent struct {
	Event         string             `json:"event"`
	Path          string             `json:"path"`
	Corrupted     bool               `json:"corrupted"`
	CorruptedOnly bool               `json:"corrupted_only"`
	Changers      []changeLogChanger `json:"changers"`
}

type changeLogChanger struct {
	Method string `json:"method"`
	ID     string `json:"id"`
}

func (r *reporterFileCtx) Finish() {
	if len(r.changers) == 0 {
		r.reporter.filesUnchanged += 1
		return
	}

	if r.reporter.encoder != nil {
		r.reporter.encoder.Encode(changeLogEvent{
			Event:         "changed",
			Path:          r.path,
			Corrupted:     r.corrupted,
			CorruptedOnly: r.corruptedOnly,
			Changers:      r.changers,
		})
	}

	r.reporter.filesChanged += 1

	if r.corrupted {
		r.reporter.filesCorrupted += 1
	}
	if r.corruptedOnly {
		r.reporter.filesCorruptedOnly += 1
	}
}

func specFromConfig(config *Config) (*ChangeTreeSpec, error) {
	steps := make([]ChangeStepSpec, len(config.Changers))
	for i, c := range config.Changers {
		var methodsFoundCount int
		var changer Changer

		id := c.ID
		if id == "" {
			id = fmt.Sprintf("#%d", i)
		}

		if c.Append != nil {
			methodsFoundCount += 1
			changer = NewAppendChanger(c.Append)
		}
		if c.Replace != nil {
			methodsFoundCount += 1
			changer = NewReplaceChanger(c.Replace)
		}
		if c.FlipByte != nil {
			methodsFoundCount += 1
			changer = NewFlipByteChanger(c.FlipByte)
		}
		if methodsFoundCount == 0 {
			return &ChangeTreeSpec{}, fmt.Errorf("Method for changer %s could not be identified", id)
		} else if methodsFoundCount > 1 {
			return &ChangeTreeSpec{}, fmt.Errorf("Two change methods for changer %s", id)
		}

		steps[i] = ChangeStepSpec{
			Changer:    changer,
			ID:         id,
			Corrupt:    c.Corrupt,
			Likelihood: c.Likelihood,
		}
	}

	var seed uint64
	if config.Seed != 0 {
		seed = config.Seed
		log.Println("Using specified seed:", seed)
	} else {
		// max is (1 << 64) - 1, resulting in the range [0, 1 << 64) for cryptoRand.Int
		max := big.NewInt(0).SetUint64(math.MaxUint64)
		bigInt, err := cryptoRand.Int(cryptoRand.Reader, max)
		if err != nil {
			return &ChangeTreeSpec{}, err
		}
		log.Println("Generated new seed:", bigInt.Uint64())

		seed = bigInt.Uint64()
	}

	return &ChangeTreeSpec{
		RootDir: config.RootDir,
		Seed:    seed,
		Steps:   steps,
	}, nil
}

type ChangeTreeSpec struct {
	RootDir         string
	Seed            uint64
	Steps           []ChangeStepSpec
	ChangeLogOutput io.Writer
}

type ChangeStepSpec struct {
	Changer    Changer
	ID         string
	Corrupt    bool
	Likelihood float64
}

func ChangeTree(spec *ChangeTreeSpec) (Summary, error) {
	src := rand.NewSource(spec.Seed)
	rnd := rand.New(src)

	contentSrc := rand.NewSource(spec.Seed)
	contentRand := rand.New(contentSrc)

	reporter := Reporter{}
	if spec.ChangeLogOutput != nil {
		reporter.encoder = json.NewEncoder(spec.ChangeLogOutput)
	}

	err := filepath.WalkDir(spec.RootDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if entry.IsDir() {
			return nil
		}

		repCtx := reporter.StartFile(path)
		defer repCtx.Finish()

		for _, c := range spec.Steps {
			if rnd.Float64() <= c.Likelihood {
				info, err := entry.Info()
				if err != nil {
					return err
				}

				ctx := &ChangeContext{
					Path:        path,
					Rand:        rnd,
					ContentRand: contentRand,
					Info:        info,
				}

				err = c.Changer.ChangeFile(ctx)
				if err != nil {
					return err
				}

				if c.Corrupt {
					err = os.Chtimes(path, time.Now(), info.ModTime())
					if err != nil {
						return err
					}
				}

				repCtx.AddChanger(c.ID, c.Changer, c.Corrupt)
			}
		}
		return nil
	})

	return reporter.Summarise(), err
}

func readConfig(path string) (*Config, error) {
	configFile, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer configFile.Close()

	config := &Config{}

	dec := yaml.NewDecoder(configFile)
	err = dec.Decode(config)
	if err != nil {
		return nil, err
	}

	return config, nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage:", os.Args[0], "<config.yaml>")
		os.Exit(1)
	}

	log.SetOutput(os.Stderr)

	config, err := readConfig(os.Args[1])
	if err != nil {
		panic(err)
	}

	changeTreeSpec, err := specFromConfig(config)
	if err != nil {
		panic(err)
	}

	changeTreeSpec.ChangeLogOutput = os.Stdout

	summary, err := ChangeTree(changeTreeSpec)
	if err != nil {
		panic(err)
	}

	log.Printf("Summary: %+v", summary)
}
