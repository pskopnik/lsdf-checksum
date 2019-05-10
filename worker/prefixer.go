package worker

import (
	"context"
	"path/filepath"
	"time"

	"github.com/apex/log"
	"gopkg.in/tomb.v2"

	"git.scc.kit.edu/sdm/lsdf-checksum/internal/cache"
	"git.scc.kit.edu/sdm/lsdf-checksum/internal/lifecycle"
	"git.scc.kit.edu/sdm/lsdf-checksum/scaleadpt"
	"git.scc.kit.edu/sdm/lsdf-checksum/workqueue"
)

type PrefixerConfig struct {
	TTL             time.Duration
	ReapingInterval time.Duration

	Logger log.Interface
}

type Prefixer struct {
	Config *PrefixerConfig

	tomb *tomb.Tomb

	mountRootCache        cache.ExpiringCache
	snapshotDirsInfoCache cache.ExpiringCache

	fieldLogger log.Interface
}

func NewPrefixer(config *PrefixerConfig) *Prefixer {
	prefixCache := &Prefixer{
		Config: config,
		mountRootCache: cache.ExpiringCache{
			TTL: config.TTL,
		},
		snapshotDirsInfoCache: cache.ExpiringCache{
			TTL: config.TTL,
		},
	}

	prefixCache.mountRootCache.Fetch = prefixCache.fetchMountRoot
	prefixCache.snapshotDirsInfoCache.Fetch = prefixCache.fetchSnapshotDirsInfo

	return prefixCache
}

func (p *Prefixer) Start(ctx context.Context) {
	p.fieldLogger = p.Config.Logger.WithFields(log.Fields{
		"component": "worker.Prefixer",
	})

	p.tomb, _ = tomb.WithContext(ctx)

	p.tomb.Go(p.reaper)
}

func (p *Prefixer) SignalStop() {
	p.tomb.Kill(lifecycle.ErrStopSignalled)
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
