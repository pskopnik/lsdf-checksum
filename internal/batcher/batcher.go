package batcher

import (
	"context"
	"errors"
	"sync"
	"time"

	pkgErrors "github.com/pkg/errors"
)

// Error variables related to batcher.
var (
	errBatcherDying             = errors.New("batcher dying / dead, cannot perform operation")
	errUnexpectedBatcherCommand = errors.New("unexpected batcher command, command is invalid in current batcherState")
	errUnknownBatcherCommand    = errors.New("unknown batcher command")
	errUnknownBatcherState      = errors.New("unknown batcher state")
	errInvalidBacherState       = errors.New("unexpected batcher state, state is not valid in this place")
)

type batcherState uint8

const (
	// batcherStateClosed means openBatch is unset and may be set by any
	// goroutine holding the mutex. Before finalising the transition (unlocking
	// the mutex), the dispatcher must be noticed by sending a
	// batcherCommandStartTimer.
	batcherStateClosed batcherState = iota
	// batcherStateOpen means openBatch may be appended to by any goroutine
	// holding the mutex. If openBatch reached MaxFiles after appending, the
	// goroutine must initiate batcherStateQueued. For the dispatcher: A timer is
	// running and will fire after MaxWaitTime.
	batcherStateOpen
	// batcherStateQueued means that openBatch must not be read or written (even
	// when holding the mutex). The queueingDone channel may be retrieved by any
	// goroutine holding the mutex. queueingDone will be closed when
	// batcherStateClosed has been reached.
	batcherStateQueued
)

type batcherCommand uint8

const (
	batcherCommandDispatch batcherCommand = 1 + iota
	batcherCommandStartTimer
	batcherCommandShutDown
	batcherCommandDispatchAndShutDown
)

type Config struct {
	MaxItems        int
	MaxWaitTime     time.Duration
	BatchBufferSize int
}

type Batcher[T any] struct {
	Config *Config

	batchPool sync.Pool

	out chan []T

	ctx            context.Context
	commandChan    chan batcherCommand
	dispatcherDone chan struct{}
	dispatcherErr  error

	mutex        sync.Mutex
	state        batcherState
	openBatch    []T
	queueingDone chan struct{}
}

func New[T any](ctx context.Context, config *Config) *Batcher[T] {
	b := &Batcher[T]{
		Config:         config,
		ctx:            ctx,
		dispatcherDone: make(chan struct{}),
		out:            make(chan []T, config.BatchBufferSize),
		commandChan:    make(chan batcherCommand),
	}

	go b.dispatcher()

	return b
}

func (b *Batcher[T]) Out() <-chan []T {
	return b.out
}

func (b *Batcher[T]) Add(ctx context.Context, item *T) error {
	var err error

	for {
		b.mutex.Lock()

		if b.state != batcherStateQueued {
			// Fast fail if the batcher is dead. On subsequent iterations this occurs
			// only after the batch transitioned from queued to closed. Not necessary
			// in batcherStateQueued.
			// TODO measure impact.
			select {
			case <-b.ctx.Done():
				b.mutex.Unlock()
				return pkgErrors.Wrap(errBatcherDying, "(*Batcher).Add")
			default:
			}
		}

		switch b.state {
		case batcherStateClosed:
			_, err = b.initiateOpeningWithLock()
			b.appendItem(item)

			b.mutex.Unlock()

			if err != nil {
				return pkgErrors.Wrap(err, "(*Batcher).Add: in state closed")
			}

			return nil
		case batcherStateOpen:
			b.appendItem(item)

			if b.openBatchComplete() {
				b.initiateQueueingWithLock()

				err = b.sendCommand(batcherCommandDispatch)
				if err != nil {
					return pkgErrors.Wrap(err, "(*Batcher).Add: in state open: trigger dispatch")
				}
			}

			b.mutex.Unlock()

			return nil
		case batcherStateQueued:
			queueingDone := b.queueingDone

			b.mutex.Unlock()

			select {
			case <-ctx.Done():
				return pkgErrors.Wrap(ctx.Err(), "(*Batcher).Add: in state queued")
			case <-b.ctx.Done():
				return pkgErrors.Wrap(errBatcherDying, "(*Batcher).Add: in state queued")
			case <-queueingDone:
			}
			continue
		default:
			return pkgErrors.Wrapf(errUnknownBatcherState, "(*Batcher).Add: encountered state %d", b.state)
		}
	}
}

// Close performs a clean close and shutdown operation. The open batch is
// closed and sent off, then the batcher will shut down.
func (b *Batcher[T]) Close(ctx context.Context) error {
	var err error

L:
	for {
		b.mutex.Lock()

		if b.state != batcherStateQueued {
			// Fast fail if the batcher is dead. On subsequent iterations this occurs
			// only after the batch transitioned from queued to closed. Not necessary
			// in batcherStateQueued.
			// TODO measure impact.
			select {
			case <-b.ctx.Done():
				b.mutex.Unlock()

				return pkgErrors.Wrap(errBatcherDying, "(*Batcher).Close")
			default:
			}
		}

		switch b.state {
		case batcherStateClosed:
			// Instead of sendCommand, the tomb could simply be killed. Here
			// sendCommand is used so that communication with the dispatcher always
			// follows the same semantic.
			err = b.sendCommand(batcherCommandShutDown)

			b.mutex.Unlock()

			if err != nil {
				return pkgErrors.Wrap(err, "(*Batcher).Close: in state close: trigger shut down")
			}

			break L
		case batcherStateOpen:
			b.initiateQueueingWithLock()

			err = b.sendCommand(batcherCommandDispatchAndShutDown)

			b.mutex.Unlock()

			if err != nil {
				return pkgErrors.Wrap(err, "(*Batcher).Close: in state open: trigger dispatch and shut down")
			}

			break L
		case batcherStateQueued:
			queueingDone := b.queueingDone

			b.mutex.Unlock()

			select {
			case <-ctx.Done():
				return pkgErrors.Wrap(ctx.Err(), "(*Batcher).Close: in state queued")
			case <-b.ctx.Done():
				return pkgErrors.Wrap(errBatcherDying, "(*Batcher).Close: in state queued")
			case <-queueingDone:
			}
			continue
		default:
			b.mutex.Unlock()
			return pkgErrors.Wrapf(errUnknownBatcherState, "(*Batcher).Close: encountered state %d", b.state)
		}
	}

	select {
	case <-b.dispatcherDone:
	case <-ctx.Done():
		return ctx.Err()
	}

	return b.dispatcherErr
}

func (b *Batcher[T]) dispatcher() {
	var err error
	var ok bool
	var cmd batcherCommand

	done := b.ctx.Done()
	state := batcherStateClosed
	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}

L:
	for {
		switch state {
		case batcherStateOpen:
			select {
			case <-done:
				if !timer.Stop() {
					<-timer.C
				}

				err = b.ctx.Err()
				break L
			case cmd = <-b.commandChan:
				// either the batch is complete or the dispatcher will die, so stop the
				// timer now
				if !timer.Stop() {
					<-timer.C
				}

				switch cmd {
				case batcherCommandDispatch:
					select {
					case <-done:
						err = b.ctx.Err()
						break L
					case b.out <- b.openBatch:
						state = b.finaliseQueueing()
						continue
					}
				case batcherCommandStartTimer:
					err = pkgErrors.Wrapf(errUnexpectedBatcherCommand, "(*Batcher).dispatcher: in state open, encountered command %d", cmd)
					break L
				case batcherCommandShutDown:
					err = pkgErrors.Wrapf(errUnexpectedBatcherCommand, "(*Batcher).dispatcher: in state open, encountered command %d", cmd)
					break L
				case batcherCommandDispatchAndShutDown:
					select {
					case <-done:
						err = b.ctx.Err()
						break L
					case b.out <- b.openBatch:
						state = b.finaliseQueueing()
						break L
					}
				default:
					err = pkgErrors.Wrapf(errUnknownBatcherCommand, "(*Batcher).dispatcher: in state open, encountered command %d", cmd)
					break L
				}
			case <-timer.C:
				b.mutex.Lock()

				state, ok = b.fastSendWithLock()
				if ok {
					b.mutex.Unlock()
					continue
				}

				state = b.initiateQueueingWithLock()

				b.mutex.Unlock()

				select {
				case <-done:
					err = b.ctx.Err()
					break L
				case b.out <- b.openBatch:
					state = b.finaliseQueueing()
					continue
				}
			}
		case batcherStateClosed:
			select {
			case <-done:
				err = b.ctx.Err()
				break L
			case cmd = <-b.commandChan:
				switch cmd {
				case batcherCommandDispatch:
					err = pkgErrors.Wrapf(errUnexpectedBatcherCommand, "(*Batcher).dispatcher: in state closed, encountered command %d", cmd)
					break L
				case batcherCommandStartTimer:
					timer.Reset(b.Config.MaxWaitTime)
					state = batcherStateOpen

					continue
				case batcherCommandShutDown:
					break L
				case batcherCommandDispatchAndShutDown:
					err = pkgErrors.Wrapf(errUnexpectedBatcherCommand, "(*Batcher).dispatcher: in state closed, encountered command %d", cmd)
					break L
				default:
					err = pkgErrors.Wrapf(errUnknownBatcherCommand, "(*Batcher).dispatcher: in state closed, encountered command %d", cmd)
					break L
				}

			}
		case batcherStateQueued:
			// this should never happen
			err = pkgErrors.Wrapf(errInvalidBacherState, "(*Batcher).dispatcher: encountered state %d", state)
			break L
		default:
			err = pkgErrors.Wrapf(errUnknownBatcherState, "(*Batcher).dispatcher: encountered state %d", state)
			break L
		}
	}

	b.dispatcherErr = err
	close(b.out)
	close(b.dispatcherDone)
}

// fastSendWithLock attempts to send the openBatch to the out channel. If the
// batch cannot be send immediately, batcherStateQueued, false is returned. If
// the batch was send, the batcher is transitioned to batcherStateClosed and
// batcherStateClosed, true is returned.
//
// fastSendWithLock must be called while holding the mutex.
// fastSendWithLock is a dispatcher function and should not be used anywhere
// else.
func (b *Batcher[T]) fastSendWithLock() (batcherState, bool) {
	select {
	case b.out <- b.openBatch:
		b.state = batcherStateClosed
		b.openBatch = nil
		return batcherStateClosed, true
	default:
	}

	return batcherStateQueued, false
}

// initiateQueueingWithLock transitions the batcher to the batcherStateQueued
// state. It always returns batcherStateQueued.
//
// initiateQueueingWithLock must be called while holding the mutex.
func (b *Batcher[T]) initiateQueueingWithLock() batcherState {
	b.state = batcherStateQueued
	b.queueingDone = make(chan struct{})

	return batcherStateQueued
}

// finaliseQueueing fully transitions the batcher from the batcherStateQueued
// to the batcherStateClosed state. For this purpose, it acquires and releases
// the mutex by itself. It always returns batcherStateClosed.
//
// finaliseQueueing is a dispatcher function and should not be used anywhere
// else.
func (b *Batcher[T]) finaliseQueueing() batcherState {
	b.mutex.Lock()

	queueingDone := b.queueingDone
	b.state = batcherStateClosed
	b.openBatch = nil
	b.queueingDone = nil

	b.mutex.Unlock()

	close(queueingDone)

	return batcherStateClosed
}

// initiateOpeningWithLock transitions the batcher to the batcherStateOpen
// state and signals the dispatcher to start its timer. It always returns
// batcherStateOpen. Additionally, a non-nil error is returned if sending the
// StartTimer command to the dispatcher failed.
//
// initiateOpeningWithLock must be called while holding the mutex.
// initiateOpeningWithLock communicates with the dispatcher and must not be
// used in the dispatcher.
func (b *Batcher[T]) initiateOpeningWithLock() (batcherState, error) {
	b.state = batcherStateOpen
	b.openBatch = []T{}

	err := b.sendCommand(batcherCommandStartTimer)
	if err != nil {
		return batcherStateOpen, pkgErrors.Wrap(err, "(*Batcher).initiateOpeningWithLock")
	}

	return batcherStateOpen, nil
}

func (b *Batcher[T]) openBatchComplete() bool {
	return len(b.openBatch) >= b.Config.MaxItems
}

func (b *Batcher[T]) sendCommand(cmd batcherCommand) error {
	select {
	case b.commandChan <- cmd:
		return nil
	case <-b.ctx.Done():
		return pkgErrors.Wrap(errBatcherDying, "(*Batcher).sendCommand")
	}
}

func (b *Batcher[T]) appendItem(item *T) {
	b.openBatch = append(b.openBatch, *item)
}
