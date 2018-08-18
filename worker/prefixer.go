package worker

import (
	"context"
	"path/filepath"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"gopkg.in/tomb.v2"

	"git.scc.kit.edu/sdm/lsdf-checksum/master/workqueue"
	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt"
)

type PrefixerConfig struct {
	TTL             time.Duration
	ReapingInterval time.Duration

	Logger logrus.FieldLogger
}

type Prefixer struct {
	Config *PrefixerConfig

	tomb *tomb.Tomb

	mountRootCache        expiringCache
	snapshotDirsInfoCache expiringCache

	fieldLogger logrus.FieldLogger
}

func NewPrefixer(config *PrefixerConfig) *Prefixer {
	prefixCache := &Prefixer{
		Config: config,
		mountRootCache: expiringCache{
			TTL: config.TTL,
		},
		snapshotDirsInfoCache: expiringCache{
			TTL: config.TTL,
		},
	}

	prefixCache.mountRootCache.Fetch = prefixCache.fetchMountRoot
	prefixCache.snapshotDirsInfoCache.Fetch = prefixCache.fetchSnapshotDirsInfo

	return prefixCache
}

func (p *Prefixer) Start(ctx context.Context) {
	p.fieldLogger = p.Config.Logger.WithFields(logrus.Fields{
		"package":   "worker",
		"component": "Prefixer",
	})

	p.tomb, _ = tomb.WithContext(ctx)

	p.tomb.Go(p.reaper)
}

func (p *Prefixer) SignalStop() {
	p.tomb.Kill(stopSignalled)
}

func (p *Prefixer) Wait() error {
	return p.tomb.Wait()
}

func (p *Prefixer) Dead() <-chan struct{} {
	return p.tomb.Dead()
}

func (p *Prefixer) Err() error {
	return p.tomb.Err()
}

func (p *Prefixer) fetchSnapshotDirsInfo(fileSystemNameIntf interface{}) (interface{}, error) {
	fileSystemName := fileSystemNameIntf.(string)

	fileSystem := scaleadpt.OpenFileSystem(fileSystemName)

	return fileSystem.GetSnapshotDirsInfo()
}

func (p *Prefixer) fetchMountRoot(fileSystemNameIntf interface{}) (interface{}, error) {
	fileSystemName := fileSystemNameIntf.(string)

	fileSystem := scaleadpt.OpenFileSystem(fileSystemName)

	return fileSystem.GetMountRoot()
}

func (p *Prefixer) getSnapshotDirsInfo(fileSystemName string) (*scaleadpt.SnapshotDirsInfo, error) {
	snapDirsInfoIntf, err := p.snapshotDirsInfoCache.Lookup(fileSystemName)
	if err != nil {
		return nil, err
	}

	snapDirsInfo := snapDirsInfoIntf.(*scaleadpt.SnapshotDirsInfo)

	return snapDirsInfo, nil
}

func (p *Prefixer) getMountRoot(fileSystemName string) (string, error) {
	mountRootIntf, err := p.mountRootCache.Lookup(fileSystemName)
	if err != nil {
		return "", err
	}

	mountRoot := mountRootIntf.(string)

	return mountRoot, nil
}

// Prefix calculates and returns the path prefix for all files in workPack.
// Prefix uses all available caches and fetches authoritative data if no cached
// data is available.
// Prefix is save to be called concurrently.
func (p *Prefixer) Prefix(workPack *workqueue.WorkPack) (string, error) {
	root, err := p.getMountRoot(workPack.FileSystemName)
	if err != nil {
		return "", err
	}
	snapDirsInfo, err := p.getSnapshotDirsInfo(workPack.FileSystemName)
	if err != nil {
		return "", err
	}

	prefix := filepath.Join(root, snapDirsInfo.Global, workPack.SnapshotName)
	return prefix, nil
}

func (p *Prefixer) reaper() error {
	dying := p.tomb.Dying()
	timer := time.NewTimer(time.Duration(0))

	p.fieldLogger.Info("Starting reaper loop")

	// Exhaust timer
	if !timer.Stop() {
		<-timer.C
	}
L:
	for {
		timer.Reset(p.Config.ReapingInterval)

		select {
		case <-timer.C:
			p.fieldLogger.Debug("Starting reap")

			entriesRemoved := p.reap()

			p.fieldLogger.WithField("entries_removed", entriesRemoved).Debug("Finished reap")
		case <-dying:
			// Exhaust timer
			if !timer.Stop() {
				<-timer.C
			}

			break L
		}
	}

	p.fieldLogger.WithField("action", "stopping").Info("Finished reaper loop")

	return nil
}

func (p *Prefixer) reap() uint {
	var totalRemoved uint

	totalRemoved += p.mountRootCache.RemoveExpired()
	totalRemoved += p.snapshotDirsInfoCache.RemoveExpired()

	return totalRemoved
}

type cacheEntry struct {
	Written time.Time
	Value   interface{}
	Deleted bool
	Lock    sync.RWMutex
}

type expiringCache struct {
	cache sync.Map
	TTL   time.Duration
	Fetch func(key interface{}) (interface{}, error)
}

func (e *expiringCache) Lookup(key interface{}) (interface{}, error) {
	now := time.Now()

	value, ok, entry := e.loadValueAndEntry(key, now)
	if ok {
		return value, nil
	}

	if entry == nil {
		value, ok, entry = e.allocateAndStoreLockedEntry(key, now)
		if ok {
			return value, nil
		}
	} else {
		// Lock entry returned by loadValueAndEntry()
		entry.Lock.Lock()
	}

	if e.isEntryValid(entry, now) {
		value = entry.Value
		entry.Lock.Unlock()

		return value, nil
	}

	value, err := e.Fetch(key)
	if err != nil {
		entry.Lock.Unlock()
		return nil, err
	}

	entry.Deleted = false
	entry.Written = now
	entry.Value = value

	entry.Lock.Unlock()

	return value, nil
}

func (e *expiringCache) loadValueAndEntry(
	key interface{}, now time.Time,
) (interface{}, bool, *cacheEntry) {
	var entry *cacheEntry

	entryIntf, ok := e.cache.Load(key)
	if ok {
		entry = entryIntf.(*cacheEntry)

		entry.Lock.RLock()

		if e.isEntryValid(entry, now) {
			value := entry.Value
			entry.Lock.RUnlock()

			return value, true, nil
		} else {
			entry.Lock.RUnlock()

		}
	}

	return nil, false, entry
}

func (e *expiringCache) allocateAndStoreLockedEntry(
	key interface{}, now time.Time,
) (interface{}, bool, *cacheEntry) {
	entry := &cacheEntry{}

	entry.Lock.Lock()

	loadedEntryIntf, loaded := e.cache.LoadOrStore(key, entry)
	if loaded {
		// Unlock entry to be discarded... just for form
		entry.Lock.Unlock()

		entry = loadedEntryIntf.(*cacheEntry)

		entry.Lock.RLock()

		if e.isEntryValid(entry, now) {
			value := entry.Value
			entry.Lock.RUnlock()

			return value, true, nil
		} else {
			entry.Lock.RUnlock()

			entry.Lock.Lock()
		}
	}

	return nil, false, entry
}

func (e *expiringCache) isEntryValid(entry *cacheEntry, now time.Time) bool {
	if now.IsZero() {
		now = time.Now()
	}
	return !entry.Deleted && entry.Written.Add(e.TTL).After(now)
}

func (e *expiringCache) RemoveExpired() uint {
	var removed uint
	var toBeDeleted bool

	now := time.Now()

	e.cache.Range(func(key interface{}, value interface{}) bool {
		entry := value.(*cacheEntry)

		entry.Lock.RLock()

		toBeDeleted = entry.Written.Add(e.TTL).Before(now)

		entry.Lock.RUnlock()

		if toBeDeleted {
			entry.Lock.Lock()

			// Check again
			if !entry.Written.Add(e.TTL).Before(now) {
				entry.Lock.Unlock()
				return true
			}

			entry.Deleted = true
			e.cache.Delete(key)
			removed++

			entry.Lock.Unlock()
		}

		return true
	})

	return removed
}
