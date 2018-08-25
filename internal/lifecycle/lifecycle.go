// Package lifecycle contains life cycle utilities for autonomous components.
//
// The definitions in this package complement the tomb package.
package lifecycle

import (
	"context"
	"errors"
)

var (
	ErrStopSignalled = errors.New("Stop signalled.")
)

type AutonomousComponent interface {
	Start(ctx context.Context)
	SignalStop()
	Wait() error
	Err() error
	Dead() <-chan struct{}
}
