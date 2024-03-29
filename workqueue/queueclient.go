package workqueue

import (
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
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
	info, err := gocraftWorkQueueInfo(q.w.pool, q.w.workNamespace, q.name)
	if err != nil {
		return QueueInfo{}, fmt.Errorf("QueueClient.GetQueueInfo: %w", err)
	}
	return info, nil
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
		return QueueWorkerInfo{}, fmt.Errorf("QueueClient.GetWorkerInfo: %w", err)
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

func (q *QueueClient[T]) IsPaused() (bool, error) {
	conn := q.w.pool.Get()
	defer conn.Close()

	pauseKey := gocraftWorkJobPauseKey(q.w.workNamespace, q.name)
	// the key's value does not matter
	count, err := redis.Int(conn.Do("EXISTS", pauseKey))
	if err != nil {
		return false, fmt.Errorf("QueueClient.IsPaused: %w", err)
	}

	return count > 0, nil
}

func (q *QueueClient[T]) Pause() error {
	conn := q.w.pool.Get()
	defer conn.Close()

	pauseKey := gocraftWorkJobPauseKey(q.w.workNamespace, q.name)
	// the key's value does not matter
	_, err := conn.Do("SET", pauseKey, "1")
	if err != nil {
		return fmt.Errorf("QueueClient.Pause: %w", err)
	}

	return nil
}

func (q *QueueClient[T]) Unpause() error {
	conn := q.w.pool.Get()
	defer conn.Close()

	pauseKey := gocraftWorkJobPauseKey(q.w.workNamespace, q.name)
	_, err := conn.Do("DEL", pauseKey)
	if err != nil {
		return fmt.Errorf("QueueClient.Unpause: %w", err)
	}

	return nil
}

func (q *QueueClient[T]) Enqueuer() *Enqueuer[T] {
	return &Enqueuer[T]{
		w:         q.w,
		enqueuer:  q.w.getEnqueuer(),
		queueName: q.name,
	}
}
