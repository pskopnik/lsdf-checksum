package ledis

import (
	"context"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/apex/log"
	"gopkg.in/tomb.v2"

	"github.com/siddontang/ledisdb/config"
	"github.com/siddontang/ledisdb/server"

	"git.scc.kit.edu/sdm/lsdf-checksum/internal/lifecycle"
)

//go:generate confions config LedisDBConfig

type LedisDBConfig struct {
	Address      string
	AuthPassword string

	ConnReadBufferSize    int
	ConnWriteBufferSize   int
	ConnKeepaliveInterval int

	TTLCheckInterval int

	// LevelDB settings

	Compression     bool
	BlockSize       int
	WriteBufferSize int
	CacheSize       int

	Logger log.Interface `yaml:"-"`
}

var LedisDBDefaultConfig = &LedisDBConfig{
	ConnReadBufferSize:  4 * 1024,
	ConnWriteBufferSize: 4 * 1024,
	TTLCheckInterval:    1,
	Compression:         false,
	BlockSize:           4 * 1024,
	WriteBufferSize:     4 * 1024 * 1024,
	CacheSize:           4 * 1024 * 1024,
}

type LedisDB struct {
	Config *LedisDBConfig

	app *server.App

	tomb *tomb.Tomb

	fieldLogger log.Interface
}

func NewLedisDB(config *LedisDBConfig) *LedisDB {
	return &LedisDB{
		Config: config,
	}
}

func (l *LedisDB) Start(ctx context.Context) {
	l.fieldLogger = l.Config.Logger.WithFields(log.Fields{
		"component": "ledis.LedisDB",
	})

	l.tomb, _ = tomb.WithContext(ctx)

	var appStored sync.WaitGroup
	appStored.Add(1)

	l.tomb.Go(func() error {
		l.tomb.Go(func() error {
			var err error

			tempDir, err := ioutil.TempDir("", "ledisdb-memstorage-datadir-")
			if err != nil {
				appStored.Done()
				return err
			}

			cfg := l.buildLedisConfig(tempDir)

			l.app, err = server.NewApp(cfg)
			appStored.Done()
			if err != nil {
				return err
			}

			l.app.Run()

			err = os.RemoveAll(tempDir)
			if err != nil {
				return err
			}

			return nil
		})

		appStored.Wait()

		l.tomb.Go(l.appStopper)

		return nil
	})

	appStored.Wait()
}

func (l *LedisDB) buildLedisConfig(tempDir string) *config.Config {
	cfg := config.NewConfigDefault()

	cfg.Databases = 1

	cfg.DataDir = tempDir
	cfg.DBPath = tempDir
	cfg.DBName = "memory"

	cfg.Addr = l.Config.Address

	cfg.ConnReadBufferSize = l.Config.ConnReadBufferSize
	cfg.ConnWriteBufferSize = l.Config.ConnWriteBufferSize
	cfg.ConnKeepaliveInterval = l.Config.ConnKeepaliveInterval

	cfg.TTLCheckInterval = l.Config.TTLCheckInterval

	cfg.LevelDB.Compression = l.Config.Compression
	cfg.LevelDB.BlockSize = l.Config.BlockSize
	cfg.LevelDB.WriteBufferSize = l.Config.WriteBufferSize
	cfg.LevelDB.CacheSize = l.Config.CacheSize

	return cfg
}

func (l *LedisDB) SignalStop() {
	l.tomb.Kill(lifecycle.ErrStopSignalled)
}

func (l *LedisDB) Wait() error {
	return l.tomb.Wait()
}

func (l *LedisDB) Dead() <-chan struct{} {
	return l.tomb.Dead()
}

func (l *LedisDB) Err() error {
	return l.tomb.Err()
}

func (l *LedisDB) Network() string {
	if l.app == nil {
		return ""
	}

	address := l.app.Address()
	if strings.Contains(address, "/") {
		return "unix"
	}

	return "tcp"
}

func (l *LedisDB) Address() string {
	if l.app == nil {
		return ""
	}

	return l.app.Address()
}

func (l *LedisDB) Addr() net.Addr {
	return &address{
		address: l.Address(),
		network: l.Network(),
	}
}

func (l *LedisDB) appStopper() error {
	<-l.tomb.Dying()

	if l.app != nil {
		l.app.Close()
	}

	return tomb.ErrDying
}

var _ net.Addr = &address{}

type address struct {
	network string
	address string
}

func (a *address) Network() string {
	return a.network
}

func (a *address) String() string {
	return a.address
}
