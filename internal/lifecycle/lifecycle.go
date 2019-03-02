// Package lifecycle contains life cycle utilities for autonomous components.
//
// The definitions in this package complement the tomb package.
package lifecycle

import (
	"context"
	"errors"
)

// Error variables related to AutonmousComponent.
var (
	ErrStopSignalled = errors.New("stop signalled")
)

type AutonomousComponent interface {
	Start(ctx context.Context)
	SignalStop()
	Wait() error
	Err() error
	Dead() <-chan struct{}
}
