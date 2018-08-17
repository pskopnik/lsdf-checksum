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
	// TODO
	// Make safe for concurrent use: Either:
	//  * Protect by Mutex / atomic
	//  * Simply copy and then re-Store()

	Written time.Time
	Value   interface{}
}

type expiringCache struct {
	cache sync.Map
	TTL   time.Duration
	Fetch func(key interface{}) (interface{}, error)
}

func (e *expiringCache) Lookup(key interface{}) (interface{}, error) {
	var entry *cacheEntry

	now := time.Now()

	entryIntf, ok := e.cache.Load(key)
	if ok {
		entry = entryIntf.(*cacheEntry)

		if entry.Written.Add(e.TTL).After(now) {
			// Valid
			return entry.Value, nil
		}
	}

	value, err := e.Fetch(key)
	if err != nil {
		return nil, err
	}

	if entry == nil {
		entry = &cacheEntry{}
		entry.Written = now
		entry.Value = value
		e.cache.Store(key, entry)
	} else {
		entry.Written = now
		entry.Value = value
	}

	return value, nil
}

func (e *expiringCache) RemoveExpired() uint {
	var removed uint

	now := time.Now()

	e.cache.Range(func(key interface{}, value interface{}) bool {
		entry := value.(*cacheEntry)

		if entry.Written.Add(e.TTL).Before(now) {
			e.cache.Delete(key)
			removed++
		}

		return true
	})

	return removed
}
