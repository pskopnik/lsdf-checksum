package redis

import (
	"context"
	"errors"
	"time"

	"github.com/gomodule/redigo/redis"
	pkgErrors "github.com/pkg/errors"
	"github.com/pskopnik/rewledis"
)

// Error variables related to Config and CreatePool.
var (
	ErrUnsupportedDialect = errors.New("specified redis dialect is not supported")
)

//go:generate confions config Config

// Config contains configuration options for a connection pool to a redis
// database.
type Config struct {
	Dialect           string
	Network           string
	Address           string
	Database          int
	Password          string
	MaxIdle           int
	IdleTimeout       time.Duration
	InternalMaxActive int
}

var DefaultConfig = &Config{
	Dialect:     "redis",
	MaxIdle:     10,
	IdleTimeout: 300 * time.Second,
}

// CreatePool creates and returns a new redis.Pool from the passed in Config.
// An error is returned if Config contains invalid or confliciting values.
// The Pool is not "tested".
func CreatePool(config *Config) (*redis.Pool, error) {
	if config.Dialect == "redis" {
		return &redis.Pool{
			Dial: func() (redis.Conn, error) {
				return redis.Dial(
					config.Network,
					config.Address,
					redis.DialDatabase(config.Database),
					redis.DialPassword(config.Password),
				)
			},
			MaxIdle:     config.MaxIdle,
			IdleTimeout: config.IdleTimeout,
		}, nil
	} else if config.Dialect == "ledis" {
		return rewledis.NewPool(&rewledis.PoolConfig{
			Dial: func() (redis.Conn, error) {
				return redis.Dial(
					config.Network,
					config.Address,
					redis.DialDatabase(config.Database),
					redis.DialPassword(config.Password),
				)
			},
			MaxIdle:     config.MaxIdle,
			IdleTimeout: config.IdleTimeout,
		}, config.InternalMaxActive), nil
	} else {
		return nil, pkgErrors.Wrapf(ErrUnsupportedDialect, "redis.CreatePool: %q dialect", config.Dialect)
	}
}

// TestPool tries to perform a PING command on a connection retrieved from pool.
// Any error is wrapped and returned.
func TestPool(ctx context.Context, pool *redis.Pool) error {
	ctx, cancel := context.WithCancel(ctx)
	conn, err := pool.GetContext(ctx)
	cancel()
	if err != nil {
		return pkgErrors.Wrap(err, "redis.TestPool: getting connection from pool failed")
	}

	_, err = conn.Do("PING")
	_ = conn.Close()
	if err != nil {
		return pkgErrors.Wrap(err, "redis.TestPool: performing PING failed")
	}

	return nil
}
