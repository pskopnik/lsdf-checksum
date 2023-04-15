// Package workqueue contains clients for the workqueue system of
// lsdf-checksum.
//
// Sub-packages provide additional key functionality.
package workqueue

import (
	"sync"

	"github.com/gocraft/work"
	"github.com/gomodule/redigo/redis"
)

const (
	gocraftWorkNamespaceBase = "lsdf-checksum/workqueue:work"
	dconfigNamespaceBase     = "lsdf-checksum/workqueue:dconfig"
)

func GocraftWorkNamespace(prefix string) string {
	l := len(prefix)
	if len(prefix) > 0 && prefix[l-1] == ':' {
		prefix = prefix[:l-1]
	}

	return prefix + ":" + gocraftWorkNamespaceBase
}

func dConfigNamespace(prefix string) string {
	l := len(prefix)
	if len(prefix) > 0 && prefix[l-1] == ':' {
		prefix = prefix[:l-1]
	}

	return prefix + ":" + dconfigNamespaceBase
}

// Workqueue is a client to a single workqueue instance under control of a
// coordinator/master.
type Workqueue struct {
	pool *redis.Pool

	instKey        string
	fileSystemName string
	snapshotName   string

	workNamespace    string
	dconfigNamespace string
	client           *work.Client
	initEnqueuer     sync.Once
	enqueuer         *work.Enqueuer
}

func New(pool *redis.Pool, prefix, fileSystemName, snapshotName string) *Workqueue {
	namespace := GocraftWorkNamespace(prefix)

	return &Workqueue{
		pool:             pool,
		fileSystemName:   fileSystemName,
		snapshotName:     snapshotName,
		workNamespace:    namespace,
		dconfigNamespace: dConfigNamespace(prefix),
		client:           work.NewClient(namespace, pool),
	}
}

func (w *Workqueue) Queues() WorkqueueQueues {
	return WorkqueueQueues{w}
}

func (w *Workqueue) DConfig() DConfigClient {
	return DConfigClient{
		w: w,
	}
}

func (w *Workqueue) getEnqueuer() *work.Enqueuer {
	w.initEnqueuer.Do(func() {
		w.enqueuer = work.NewEnqueuer(w.workNamespace, w.pool)
	})

	return w.enqueuer
}

type WorkqueueQueues struct {
	w *Workqueue
}

func (w WorkqueueQueues) ComputeChecksum() *QueueClient[*WorkPack] {
	return &QueueClient[*WorkPack]{
		name: ComputeChecksumJobName,
		w:    w.w,
	}
}

func (w WorkqueueQueues) WriteBack() *QueueClient[*WriteBackPack] {
	return &QueueClient[*WriteBackPack]{
		name: WriteBackJobName(w.w.fileSystemName, w.w.snapshotName),
		w:    w.w,
	}
}
