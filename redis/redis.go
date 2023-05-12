package redis

import (
	"context"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
)

//go:generate confions config Config

// Config contains configuration options for a connection pool to a redis
// database.
type Config struct {
	Network           string
	Address           string
	Database          int
	Username          string
	Password          string
	MaxIdle           int
	IdleTimeout       time.Duration
	InternalMaxActive int
}

var DefaultConfig = &Config{
	MaxIdle:     10,
	IdleTimeout: 300 * time.Second,
}

// CreatePool creates and returns a new redis.Pool from the passed in Config.
// An error is returned if Config contains invalid or confliciting values.
// The Pool is not "tested".
func CreatePool(config *Config) (*redis.Pool, error) {
	return &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.Dial(
				config.Network,
				config.Address,
				redis.DialDatabase(config.Database),
				redis.DialUsername(config.Username),
				redis.DialPassword(config.Password),
			)
		},
		MaxIdle:     config.MaxIdle,
		IdleTimeout: config.IdleTimeout,
	}, nil
}

// TestPool tries to perform a PING command on a connection retrieved from pool.
// Any error is wrapped and returned.
func TestPool(ctx context.Context, pool *redis.Pool) error {
	ctx, cancel := context.WithCancel(ctx)
	conn, err := pool.GetContext(ctx)
	cancel()
	if err != nil {
		return errors.Wrap(err, "redis.TestPool: getting connection from pool failed")
	}

	_, err = conn.Do("PING")
	_ = conn.Close()
	if err != nil {
		return errors.Wrap(err, "redis.TestPool: performing PING failed")
	}

	return nil
}
