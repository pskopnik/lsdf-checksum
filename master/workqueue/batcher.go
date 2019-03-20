package workqueue

import (
	"context"
	"errors"
	"sync"
	"time"

	pkgErrors "github.com/pkg/errors"
	"gopkg.in/tomb.v2"

	"git.scc.kit.edu/sdm/lsdf-checksum/internal/lifecycle"
)

// Error variables related to batcher.
var (
	errBatcherDying             = errors.New("batcher dying / dead, cannot perform operation")
	errUnexpectedBatcherCommand = errors.New("unexpected batcher command, command is invalid in current batcherState")
	errUnknownBatcherCommand    = errors.New("unknown batcher command")
	errUnknownBatcherState      = errors.New("unknown batcher state")
	errInvalidBacherState       = errors.New("unexpected batcher state, state is not valid in this place")
)

type filesBatch struct {
	batcher *batcher
	Files   []WriteBackPackFile
}

// Return returns the batch to the pool held by the batcher. After Return has
// been called, the fields of this instance must not be accessed.
func (f *filesBatch) Return() {
	if f.batcher != nil {
		f.batcher.returnToPool(f)
	}
}

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

type batcherConfig struct {
	MaxItems        int
	MaxWaitTime     time.Duration
	BatchBufferSize int
}

type batcher struct {
	Config *batcherConfig

	batchPool sync.Pool

	out chan *filesBatch

	tomb        *tomb.Tomb
	commandChan chan batcherCommand

	mutex        sync.Mutex
	state        batcherState
	openBatch    *filesBatch
	queueingDone chan struct{}
}

func newBatcher(config *batcherConfig) *batcher {
	return &batcher{
		Config: config,
	}
}

func (b *batcher) Start(ctx context.Context) {
	b.tomb, _ = tomb.WithContext(ctx)
	b.out = make(chan *filesBatch, b.Config.BatchBufferSize)
	b.commandChan = make(chan batcherCommand)
	b.tomb.Go(b.dispatcher)
}

func (b *batcher) SignalStop() {
	b.tomb.Kill(lifecycle.ErrStopSignalled)
}

func (b *batcher) Wait() error {
	return b.tomb.Wait()
}

func (b *batcher) Dead() <-chan struct{} {
	return b.tomb.Dead()
}

func (b *batcher) Err() error {
	return b.tomb.Err()
}

func (b *batcher) Out() <-chan *filesBatch {
	return b.out
}

func (b *batcher) Add(ctx context.Context, file *WriteBackPackFile) error {
	var err error

	for {
		b.mutex.Lock()

		if b.state != batcherStateQueued {
			// Fast fail if the batcher is dead. On subsequent iterations this occurs
			// only after the batch transitioned from queued to closed. Not necessary
			// in batcherStateQueued.
			// TODO measure impact.
			select {
			case <-b.tomb.Dying():
				return pkgErrors.Wrap(errBatcherDying, "(*batcher).Add")
			default:
			}
		}

		switch b.state {
		case batcherStateClosed:
			_, err = b.initiateOpeningWithLock()
			b.appendFile(file)

			b.mutex.Unlock()

			if err != nil {
				return pkgErrors.Wrap(err, "(*batcher).Add: in state closed")
			}

			return nil
		case batcherStateOpen:
			b.appendFile(file)

			if b.openBatchComplete() {
				b.initiateQueueingWithLock()

				err = b.sendCommand(batcherCommandDispatch)
				if err != nil {
					return pkgErrors.Wrap(err, "(*batcher).Add: in state open: trigger dispatch")
				}
			}

			b.mutex.Unlock()

			return nil
		case batcherStateQueued:
			queueingDone := b.queueingDone

			b.mutex.Unlock()

			select {
			case <-ctx.Done():
				return pkgErrors.Wrap(ctx.Err(), "(*batcher).Add: in state queued")
			case <-b.tomb.Dying():
				return pkgErrors.Wrap(errBatcherDying, "(*batcher).Add: in state queued")
			case <-queueingDone:
			}
			continue
		default:
			return pkgErrors.Wrapf(errUnknownBatcherState, "(*batcher).Add: encountered state %d", b.state)
		}
	}
}

// Close performs a clean close and shutdown operation. The open batch is
// closed and sent off, then the batcher will shut down.
//
// SignalStop() should be used for fast shut down.
func (b *batcher) Close(ctx context.Context) error {
	var err error

	for {
		b.mutex.Lock()

		if b.state != batcherStateQueued {
			// Fast fail if the batcher is dead. On subsequent iterations this occurs
			// only after the batch transitioned from queued to closed. Not necessary
			// in batcherStateQueued.
			// TODO measure impact.
			select {
			case <-b.tomb.Dying():
				return pkgErrors.Wrap(errBatcherDying, "(*batcher).Close")
			default:
			}
		}

		switch b.state {
		case batcherStateClosed:
			// Instead of sendCommand, the tomb could simply be killed. Here
			// sendCommand is used so that communication with the dispatcher always
			// follows the same semantic.
			err = b.sendCommand(batcherCommandShutDown)
			if err != nil {
				return pkgErrors.Wrap(err, "(*batcher).Close: in state close: trigger shut down")
			}

			b.mutex.Unlock()

			return nil
		case batcherStateOpen:
			b.initiateQueueingWithLock()

			err = b.sendCommand(batcherCommandDispatchAndShutDown)
			if err != nil {
				return pkgErrors.Wrap(err, "(*batcher).Close: in state open: trigger dispatch and shut down")
			}

			b.mutex.Unlock()

			return nil
		case batcherStateQueued:
			queueingDone := b.queueingDone

			b.mutex.Unlock()

			select {
			case <-ctx.Done():
				return pkgErrors.Wrap(ctx.Err(), "(*batcher).Close: in state queued")
			case <-b.tomb.Dying():
				return pkgErrors.Wrap(errBatcherDying, "(*batcher).Close: in state queued")
			case <-queueingDone:
			}
			continue
		default:
			return pkgErrors.Wrapf(errUnknownBatcherState, "(*batcher).Close: encountered state %d", b.state)
		}
	}
}

func (b *batcher) dispatcher() error {
	var err error
	var ok bool
	var cmd batcherCommand

	dying := b.tomb.Dying()
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
			case <-dying:
				if !timer.Stop() {
					<-timer.C
				}

				err = tomb.ErrDying
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
					case <-dying:
						err = tomb.ErrDying
						break L
					case b.out <- b.openBatch:
						state = b.finaliseQueueing()
						continue
					}
				case batcherCommandStartTimer:
					err = pkgErrors.Wrapf(errUnexpectedBatcherCommand, "(*batcher).dispatcher: in state open, encountered command %d", cmd)
					break L
				case batcherCommandShutDown:
					err = pkgErrors.Wrapf(errUnexpectedBatcherCommand, "(*batcher).dispatcher: in state open, encountered command %d", cmd)
					break L
				case batcherCommandDispatchAndShutDown:
					select {
					case <-dying:
						err = tomb.ErrDying
						break L
					case b.out <- b.openBatch:
						state = b.finaliseQueueingAndKill()
						break L
					}
				default:
					err = pkgErrors.Wrapf(errUnknownBatcherCommand, "(*batcher).dispatcher: in state open, encountered command %d", cmd)
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
				case <-dying:
					err = tomb.ErrDying
					break L
				case b.out <- b.openBatch:
					state = b.finaliseQueueing()
					continue
				}
			}
		case batcherStateClosed:
			select {
			case <-dying:
				err = tomb.ErrDying
				break L
			case cmd = <-b.commandChan:
				switch cmd {
				case batcherCommandDispatch:
					err = pkgErrors.Wrapf(errUnexpectedBatcherCommand, "(*batcher).dispatcher: in state closed, encountered command %d", cmd)
					break L
				case batcherCommandStartTimer:
					timer.Reset(b.Config.MaxWaitTime)
					state = batcherStateOpen

					continue
				case batcherCommandShutDown:
					break L
				case batcherCommandDispatchAndShutDown:
					err = pkgErrors.Wrapf(errUnexpectedBatcherCommand, "(*batcher).dispatcher: in state closed, encountered command %d", cmd)
					break L
				default:
					err = pkgErrors.Wrapf(errUnknownBatcherCommand, "(*batcher).dispatcher: in state closed, encountered command %d", cmd)
					break L
				}

			}
		case batcherStateQueued:
			// this should never happen
			err = pkgErrors.Wrapf(errInvalidBacherState, "(*batcher).dispatcher: encountered state %d", state)
			break L
		default:
			err = pkgErrors.Wrapf(errUnknownBatcherState, "(*batcher).dispatcher: encountered state %d", state)
			break L
		}
	}

	close(b.out)
	return err
}

// fastSendWithLock attempts to send the openBatch to the out channel. If the
// batch cannot be send immediately, batcherStateQueued, false is returned. If
// the batch was send, the batcher is transitioned to batcherStateClosed and
// batcherStateClosed, true is returned.
//
// fastSendWithLock must be called while holding the mutex.
// fastSendWithLock is a dispatcher function and should not be used anywhere
// else.
func (b *batcher) fastSendWithLock() (batcherState, bool) {
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
func (b *batcher) initiateQueueingWithLock() batcherState {
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
func (b *batcher) finaliseQueueing() batcherState {
	b.mutex.Lock()

	queueingDone := b.queueingDone
	b.state = batcherStateClosed
	b.openBatch = nil
	b.queueingDone = nil

	b.mutex.Unlock()

	close(queueingDone)

	return batcherStateClosed
}

// finaliseQueueingAndKill fully transitions the batcher from the
// batcherStateQueued to the batcherStateClosed state and kills the batcher's
// tomb. For this purpose, it acquires and releases the mutex by itself. It
// always returns batcherStateClosed.
//
// finaliseQueueingAndKill is a dispatcher function and should not be used
// anywhere else.
func (b *batcher) finaliseQueueingAndKill() batcherState {
	b.mutex.Lock()

	queueingDone := b.queueingDone
	b.state = batcherStateClosed
	b.openBatch = nil
	b.queueingDone = nil

	b.tomb.Kill(nil)

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
func (b *batcher) initiateOpeningWithLock() (batcherState, error) {
	b.state = batcherStateOpen
	b.openBatch = b.getFromPool()

	err := b.sendCommand(batcherCommandStartTimer)
	if err != nil {
		return batcherStateOpen, pkgErrors.Wrap(err, "(*batcher).initiateOpeningWithLock")
	}

	return batcherStateOpen, nil
}

func (b *batcher) openBatchComplete() bool {
	return len(b.openBatch.Files) >= b.Config.MaxItems
}

func (b *batcher) sendCommand(cmd batcherCommand) error {
	select {
	case b.commandChan <- cmd:
		return nil
	case <-b.tomb.Dying():
		return pkgErrors.Wrap(errBatcherDying, "(*batcher).sendCommand")
	}
}

func (b *batcher) appendFile(file *WriteBackPackFile) {
	if len(b.openBatch.Files)+1 <= cap(b.openBatch.Files) {
		// reuse the existing file, allows reusing (WriteBackPackFile).Checksum
		b.openBatch.Files = b.openBatch.Files[:len(b.openBatch.Files)+1]
	} else {
		b.openBatch.Files = append(b.openBatch.Files, WriteBackPackFile{})
	}

	b.openBatch.Files[len(b.openBatch.Files)-1].DeepCopyFrom(file)
}

func (b *batcher) getFromPool() *filesBatch {
	batch := b.batchPool.Get()
	if batch == nil {
		return &filesBatch{
			batcher: b,
		}
	}

	return batch.(*filesBatch)
}

func (b *batcher) returnToPool(batch *filesBatch) {
	batch.Files = batch.Files[:0]
	b.batchPool.Put(batch)
}
