package main

import (
	"bufio"
	cryptoRand "crypto/rand"
	"errors"
	"io"
	"log"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/exp/rand"

	// goErrors "github.com/go-errors/errors"
	"github.com/stretchr/objx"
	"gonum.org/v1/gonum/stat/distuv"
	"gopkg.in/yaml.v2"
)

var (
	InsufficientlySpecifiedErr = errors.New("Insufficiently specified parameters.")
)

const (
	dirPerm  = 0755
	filePerm = 0644

	pathDevUrandom = "/dev/urandom"

	// copyBufferSize is the size used for various buffers involved in reading
	// and writing files (copying file / binary data).
	copyBufferSize = 10 * 1 << (2 * 10) // 10 MB
)

var (
	copyBufferPool = &sync.Pool{
		New: func() interface{} {
			return make([]byte, copyBufferSize)
		},
	}
	bufferedWriterPool = &sync.Pool{
		New: func() interface{} {
			return bufio.NewWriterSize(nil, copyBufferSize)
		},
	}
)

type Config struct {
	RootDir string                 `yaml:"root_dir"`
	Model   string                 `yaml:"model"`
	Spec    map[string]interface{} `yaml:"spec"`
}

type Model interface {
	Generate(rootDir string) error
}

type zeroReader struct{}

func (z *zeroReader) Read(p []byte) (int, error) {
	for i := 0; i < len(p); i++ {
		p[i] = 0x0
	}

	return len(p), nil
}

type FillMode int

const (
	FM_UNSPECIFIED FillMode = iota
	FM_NOBYTES
	FM_SIZESTR
	FM_RANDOMBYTES
)

func FillModeFromString(s string) FillMode {
	switch strings.ToLower(s) {
	case "nobytes":
		return FM_NOBYTES
	case "sizestr":
		return FM_SIZESTR
	case "randombytes":
		return FM_RANDOMBYTES
	default:
		return FM_UNSPECIFIED
	}
}

type CompleteTreeModel struct {
	src  rand.Source
	rand *rand.Rand

	maxDepth                 int
	nameLength               int
	fillMode                 FillMode
	subDirectoryDistribution distuv.Rander
	fileDistribution         distuv.Rander
	fileSizeDistribution     distuv.Rander

	urandomReader *bufio.Reader
	zeroReader    zeroReader
}

func (c *CompleteTreeModel) FromSpec(spec map[string]interface{}) error {
	specMap := objx.Map(spec)

	if v := specMap.Get("seed"); v.IsUint64() {
		c.src = rand.NewSource(v.Uint64())
	} else {
		// max is (1 << 64) - 1, resulting in the range [0, 1 << 64) for cryptoRand.Int
		max := big.NewInt(0).SetUint64(math.MaxUint64)
		bigInt, err := cryptoRand.Int(cryptoRand.Reader, max)
		if err != nil {
			return err
		}
		log.Println("Generated new seed:", bigInt.Uint64())

		c.src = rand.NewSource(bigInt.Uint64())
	}

	c.rand = rand.New(c.src)

	c.maxDepth = specMap.Get("max_depth").Int(1)

	c.nameLength = specMap.Get("name_length").Int(20)

	c.fillMode = FillModeFromString(
		specMap.Get("fill_mode").Str("randombytes"),
	)

	c.subDirectoryDistribution = DistributionFromSpec(
		miiToMSI(specMap.Get("sub_directory_distribution").Data()),
		c.src,
	)
	if c.subDirectoryDistribution == nil {
		return InsufficientlySpecifiedErr
	}

	c.fileDistribution = DistributionFromSpec(
		miiToMSI(specMap.Get("file_distribution").Data()),
		c.src,
	)
	if c.fileDistribution == nil {
		return InsufficientlySpecifiedErr
	}

	c.fileSizeDistribution = DistributionFromSpec(
		miiToMSI(specMap.Get("file_size_distribution").Data()),
		c.src,
	)
	if c.fileSizeDistribution == nil {
		return InsufficientlySpecifiedErr
	}

	return nil
}

func (c *CompleteTreeModel) Generate(rootDir string) error {
	err := os.MkdirAll(rootDir, dirPerm)
	if err != nil {
		return err
	}

	devUrandom, err := os.Open(pathDevUrandom)
	if err != nil {
		return err
	}
	defer devUrandom.Close()

	if c.urandomReader == nil {
		c.urandomReader = bufio.NewReaderSize(devUrandom, copyBufferSize)
	} else {
		c.urandomReader.Reset(devUrandom)
	}
	defer c.urandomReader.Reset(nil)

	return c.recPopulateDir(rootDir, 1)
}

func (c *CompleteTreeModel) recPopulateDir(dir string, depth int) error {
	numOfFiles := int(c.fileDistribution.Rand())

	for i := 0; i < numOfFiles; i++ {
		name := c.generateName()
		_, err := os.Stat(filepath.Join(dir, name))
		for err == nil || !os.IsNotExist(err) {
			name = c.generateName()
			_, err = os.Stat(filepath.Join(dir, name))
		}
		if err != nil && !os.IsNotExist(err) {
			return err
		}

		err = c.fillFile(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}

	if depth >= c.maxDepth {
		return nil
	}

	numOfSubDirectories := int(c.subDirectoryDistribution.Rand())

	for i := 0; i < numOfSubDirectories; i++ {
		name := c.generateName()
		err := os.Mkdir(filepath.Join(dir, name), dirPerm)
		for os.IsExist(err) {
			name = c.generateName()
			err = os.Mkdir(filepath.Join(dir, name), dirPerm)
		}
		if err != nil {
			return err
		}

		err = c.recPopulateDir(filepath.Join(dir, name), depth+1)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *CompleteTreeModel) fillFile(path string) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, filePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	fileSize := int64(c.fileSizeDistribution.Rand())

	switch c.fillMode {
	case FM_RANDOMBYTES:
		bufferedFile := bufferedWriterPool.Get().(*bufio.Writer)
		bufferedFile.Reset(file)
		defer bufferedWriterPool.Put(bufferedFile)
		defer bufferedFile.Reset(nil)

		var bytesWritten, written int64
		var n int

		buf := copyBufferPool.Get().([]byte)
		defer copyBufferPool.Put(buf)

		for bytesWritten < fileSize {
			n = min(c.rand.Intn(1024), int(fileSize-bytesWritten))

			written, err = io.CopyBuffer(file, io.LimitReader(c.urandomReader, int64(n)), buf)
			if err != nil {
				return err
			}
			bytesWritten += written

			n = min(c.rand.Intn(10*1024), int(fileSize-bytesWritten))

			written, err = io.CopyBuffer(file, io.LimitReader(&c.zeroReader, int64(n)), buf)
			if err != nil {
				return err
			}
			bytesWritten += written
		}
		bufferedFile.Flush()
	case FM_SIZESTR:
		_, err = file.Write([]byte(strconv.FormatInt(fileSize, 10)))
	case FM_NOBYTES:
	}

	return nil
}

func (c *CompleteTreeModel) generateName() string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	n := c.nameLength

	buf := make([]byte, n)

	for i := 0; i < n; i++ {
		buf[i] = letters[c.rand.Intn(len(letters))]
	}

	return string(buf)
}

func DistributionFromSpec(spec map[string]interface{}, src rand.Source) distuv.Rander {
	specMap := objx.Map(spec)

	if !specMap.Get("name").IsStr() {
		return nil
	}
	if !specMap.Has("params") {
		return nil
	}

	var rander distuv.Rander

	paramsMap := objx.Map(miiToMSI(specMap.Get("params").Data()))

	switch specMap.Get("name").Str() {
	case "pareto":
		rander = &distuv.Pareto{
			Src:   src,
			Xm:    paramsMap.Get("xm").Float64(float64(paramsMap.Get("xm").Int(0))),
			Alpha: paramsMap.Get("alpha").Float64(float64(paramsMap.Get("alpha").Int(0))),
		}
	case "log_normal":
		rander = &distuv.LogNormal{
			Src:   src,
			Mu:    paramsMap.Get("mu").Float64(float64(paramsMap.Get("mu").Int(0))),
			Sigma: paramsMap.Get("sigma").Float64(float64(paramsMap.Get("sigma").Int(0))),
		}
		log.Printf("%+v", rander)
	}

	return rander
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func miiToMSI(mii interface{}) map[string]interface{} {
	typedMII, ok := mii.(map[interface{}]interface{})
	if !ok {
		return nil
	}

	msi := make(map[string]interface{})

	for key, val := range typedMII {
		if typedKey, ok := key.(string); ok {
			msi[typedKey] = val
		}
	}

	return msi
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
	config, err := readConfig(os.Args[1])
	if err != nil {
		panic(err)
	}

	var model Model

	switch config.Model {
	case "CompleteTree":
		treeModel := &CompleteTreeModel{}
		err := treeModel.FromSpec(config.Spec)
		if err != nil {
			panic(err)
		}

		model = treeModel
	default:
		panic("Unknown 'model'")
	}

	err = model.Generate(config.RootDir)
	if err != nil {
		panic(err)
	}
}
