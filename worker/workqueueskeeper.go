package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apex/log"
	"github.com/gomodule/redigo/redis"
	"golang.org/x/time/rate"

	"git.scc.kit.edu/sdm/lsdf-checksum/workqueue"
)

type workqueueKey struct {
	FileSystemName, SnapshotName string
}

type WorkqueueContext struct {
	Workqueue         *workqueue.Workqueue
	WriteBackEnqueuer *workqueue.Enqueuer[*workqueue.WriteBackPack]
	Consumer          *workqueue.DConfigConsumer
	Limiter           *rate.Limiter
}

type workqueueEntry struct {
	LastAccess time.Time

	Ctx    context.Context
	Cancel context.CancelFunc

	Workqueue         *workqueue.Workqueue
	WriteBackEnqueuer *workqueue.Enqueuer[*workqueue.WriteBackPack]
	Consumer          *workqueue.DConfigConsumer
	Watcher           *workqueue.DConfigWatcher
	Limiter           *rate.Limiter
}

func (w *workqueueEntry) ExportToContext() WorkqueueContext {
	return WorkqueueContext{
		Workqueue:         w.Workqueue,
		WriteBackEnqueuer: w.WriteBackEnqueuer,
		Consumer:          w.Consumer,
		Limiter:           w.Limiter,
	}
}

type WorkqueuesKeeperConfig struct {
	Context         context.Context
	Pool            *redis.Pool
	Prefix          string
	FileReadSize    int
	Logger          log.Interface
	Workqueue       workqueue.Config
	ReapingInterval time.Duration
}

type WorkqueuesKeeper struct {
	config WorkqueuesKeeperConfig

	fieldLogger log.Interface

	m  sync.Mutex
	ws map[workqueueKey]*workqueueEntry
}

func NewWorkqueuesKeeper(config WorkqueuesKeeperConfig) *WorkqueuesKeeper {
	return &WorkqueuesKeeper{
		config: config,
		ws:     make(map[workqueueKey]*workqueueEntry),
	}
}

func (w *WorkqueuesKeeper) Start() {
	w.fieldLogger = w.config.Logger.WithFields(log.Fields{
		"component": "worker.WorkqueuesKeeper",
	})

	go w.reaper()
}

func (w *WorkqueuesKeeper) Get(fileSystemName, snapshotName string) (WorkqueueContext, error) {
	w.m.Lock()

	key := workqueueKey{fileSystemName, snapshotName}

	if entry, ok := w.ws[key]; ok {
		entry.LastAccess = time.Now()
		wqCtx := entry.ExportToContext()

		w.m.Unlock()

		return wqCtx, nil
	}

	w.m.Unlock()

	w.fieldLogger.WithFields(log.Fields{
		"filesystem": key.FileSystemName,
		"snapshot":   key.SnapshotName,
	}).Debug("Preparing new workqueue instance")

	entry, err := w.prepareEntry(key)
	if err != nil {
		return WorkqueueContext{}, fmt.Errorf("WorkqueuesKeeper.Get: %w", err)
	}

	w.m.Lock()

	// Another goroutine could have set an entry in the meantime
	if othersEntry, ok := w.ws[key]; ok {
		othersEntry.LastAccess = time.Now()
		wqCtx := othersEntry.ExportToContext()

		w.m.Unlock()

		// Close the newly prepared entry
		w.closeEntry(entry)

		return wqCtx, nil
	}

	w.ws[key] = entry

	entry.LastAccess = time.Now()

	wqCtx := entry.ExportToContext()

	w.m.Unlock()

	return wqCtx, nil
}

func (w *WorkqueuesKeeper) prepareEntry(key workqueueKey) (*workqueueEntry, error) {
	var err error
	var entry workqueueEntry

	entry.Workqueue = workqueue.New(key.FileSystemName, key.SnapshotName, *workqueue.DefaultConfig.
		Clone().
		Merge(&w.config.Workqueue).
		Merge(&workqueue.Config{
			Pool:   w.config.Pool,
			Prefix: w.config.Prefix,
			Logger: w.config.Logger,
		}),
	)

	entry.Ctx, entry.Cancel = context.WithCancel(w.config.Context)

	entry.WriteBackEnqueuer = entry.Workqueue.Queues().WriteBack().Enqueuer()

	entry.Consumer, err = entry.Workqueue.DConfig().StartConsumer(entry.Ctx)
	if err != nil {
		return nil, fmt.Errorf("prepareEntry: %w", err)
	}

	entry.Watcher = entry.Consumer.Watch()

	entry.Limiter = rate.NewLimiter(
		w.dConfigToLimit(entry.Watcher.Get()),
		w.config.FileReadSize,
	)

	go w.updateLimiter(&entry)

	return &entry, nil
}

func (w *WorkqueuesKeeper) updateLimiter(entry *workqueueEntry) {
	for {
		select {
		case <-entry.Watcher.C():
			newLimit := w.dConfigToLimit(entry.Watcher.Get())
			if newLimit != entry.Limiter.Limit() {
				w.fieldLogger.WithFields(log.Fields{
					"new_limit": newLimit,
					"old_limit": entry.Limiter.Limit(),
					// FUTR: Add instKey
				}).Debug("Updating workqueue throughput limit")
				entry.Limiter.SetLimit(newLimit)
			}
		case <-entry.Ctx.Done():
			return
		}
	}
}

func (w *WorkqueuesKeeper) dConfigToLimit(data workqueue.DConfigData) rate.Limit {
	if data.MaxNodeThroughput > 0 {
		return rate.Limit(data.MaxNodeThroughput)
	} else {
		return rate.Inf
	}
}

func (w *WorkqueuesKeeper) closeEntry(entry *workqueueEntry) {
	entry.Cancel()
}

func (w *WorkqueuesKeeper) RemoveUnusedSince(ref time.Time) uint {
	var toClose []*workqueueEntry

	w.m.Lock()

	for key, entry := range w.ws {
		if entry.LastAccess.Before(ref) {
			toClose = append(toClose, entry)
			delete(w.ws, key)
		}
	}

	w.m.Unlock()

	for _, entry := range toClose {
		w.closeEntry(entry)
	}

	return uint(len(toClose))
}

func (w *WorkqueuesKeeper) reaper() {
	timer := time.NewTimer(0)
	// Exhaust timer
	if !timer.Stop() {
		<-timer.C
	}

	w.fieldLogger.Info("Starting reaper loop")

L:
	for {
		timer.Reset(w.config.ReapingInterval)

		select {
		case <-timer.C:
			w.fieldLogger.Debug("Starting reaping")

			entriesRemoved := w.RemoveUnusedSince(time.Now().Add(-w.config.ReapingInterval))

			w.fieldLogger.WithFields(log.Fields{
				"entries_removed": entriesRemoved,
			}).Debug("Finished reaping")
		case <-w.config.Context.Done():
			_ = timer.Stop()
			break L
		}
	}

	w.fieldLogger.WithFields(log.Fields{
		"action": "stopping",
	}).Info("Finished reaper loop")
}
