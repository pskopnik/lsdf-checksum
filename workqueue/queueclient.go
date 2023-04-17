package workqueue

import (
	"time"
)

// gocraft/work uses 10 seconds
const workerPoolDeadAge = 30 * time.Second

type QueueQuerier interface {
	Name() string
	GetQueueInfo() (QueueInfo, error)
	GetWorkerInfo() (QueueWorkerInfo, error)
}

var _ QueueQuerier = &QueueClient[*WorkPack]{}

type QueueClient[T JobPayload] struct {
	name string

	w *Workqueue
}

func (q *QueueClient[T]) Name() string {
	return q.name
}

type QueueInfo struct {
	QueuedJobs uint64
	Latency    uint64
}

func (q *QueueClient[T]) GetQueueInfo() (QueueInfo, error) {
	queues, err := q.w.client.Queues()
	if err != nil {
		return QueueInfo{}, err
	}

	for _, queue := range queues {
		if queue.JobName == q.name {
			return QueueInfo{
				QueuedJobs: uint64(queue.Count),
				Latency:    uint64(queue.Latency),
			}, nil
		}
	}

	// The queue has not been found, that means it has not been initialised yet.
	return QueueInfo{}, nil
}

type QueueWorkerInfo struct {
	WorkerNum uint64
	NodeNum   uint64
}

// GetWorkerInfo returns the number of alive workers currently registered with
// the workqueue for this queue.
// This is equal to the total processing concurrency of the queue.
func (q *QueueClient[T]) GetWorkerInfo() (QueueWorkerInfo, error) {
	heartbeats, err := q.w.client.WorkerPoolHeartbeats()
	if err != nil {
		return QueueWorkerInfo{}, err
	}

	deadThreshold := time.Now().Add(-workerPoolDeadAge).Unix()

	var workerNum, nodeNum uint64

	for _, heartbeat := range heartbeats {
		if heartbeat.HeartbeatAt < deadThreshold {
			// Worker pool has died but was not yet cleaned up
			continue
		}

		found := false
		for _, jobName := range heartbeat.JobNames {
			if jobName == q.name {
				found = true
				break
			}
		}
		if !found {
			continue
		}

		workerNum += uint64(heartbeat.Concurrency)
		nodeNum++
	}

	return QueueWorkerInfo{
		WorkerNum: workerNum,
		NodeNum:   nodeNum,
	}, nil
}

func (q *QueueClient[T]) Pause() error {
	conn := q.w.pool.Get()
	defer conn.Close()

	// the specific value does not matter
	_, err := conn.Do("SET", q.pauseKey(), "1")
	if err != nil {
		return err
	}

	return nil
}

func (q *QueueClient[T]) Unpause() error {
	conn := q.w.pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", q.pauseKey())
	if err != nil {
		return err
	}

	return nil
}

func (q *QueueClient[T]) pauseKey() string {
	// From gocraft/work.redisKeyJobsPaused

	namespace := q.w.workNamespace
	l := len(namespace)
	if l > 0 && namespace[l-1] == ':' {
		namespace = namespace[:l-1]
	}

	return namespace + ":jobs:" + q.name + ":paused"
}

func (q *QueueClient[T]) Enqueuer() *Enqueuer[T] {
	return &Enqueuer[T]{
		w:         q.w,
		enqueuer:  q.w.getEnqueuer(),
		queueName: q.name,
	}
}
