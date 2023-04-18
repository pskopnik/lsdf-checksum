package workqueue

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gocraft/work"
	"github.com/gomodule/redigo/redis"
)

// Reimplementation and improvement of some critical bits of gocraft/work.
// https://github.com/gocraft/work

func gocraftWorkJobKey(namespace, jobName string) string {
	l := len(namespace)
	if l > 0 {
		if namespace[l-1] == ':' {
			namespace = namespace[:l-1]
		}
		return namespace + ":jobs:" + jobName
	} else {
		return "jobs:" + jobName
	}
}

func gocraftWorkJobPauseKey(namespace, jobName string) string {
	return gocraftWorkJobKey(namespace, jobName) + ":paused"
}

// gocraftWorkQueueInfo queries queue-status information from the queue of
// jobName. It only queries a single queue and also fixes a race-condition in
// the implementation of [work.Client.Queues], where an error is returned if
// a queue went from > 0 to 0 jobs during the function execution. The
// function performs only one round-trip.
func gocraftWorkQueueInfo(pool *redis.Pool, namespace, jobName string) (QueueInfo, error) {
	var info QueueInfo
	var err error

	conn := pool.Get()
	defer conn.Close()

	conn.Send("LLEN", gocraftWorkJobKey(namespace, jobName))
	conn.Send("LINDEX", gocraftWorkJobKey(namespace, jobName), -1)
	if err := conn.Flush(); err != nil {
		return info, fmt.Errorf("gocraftWorkQueueInfo: flush to redis: %w", err)
	}

	info.QueuedJobs, err = redis.Uint64(conn.Receive())
	if err != nil {
		return info, fmt.Errorf("gocraftWorkQueueInfo: read count: %w", err)
	}

	buf, err := redis.Bytes(conn.Receive())
	if err == redis.ErrNil {
		// If there is no "last element", there must be no elements
		info.QueuedJobs = 0
	} else if err != nil {
		return info, fmt.Errorf("gocraftWorkQueueInfo: read next job: %w", err)
	} else {
		var job work.Job
		err = json.Unmarshal(buf, &job)
		if err != nil {
			return info, fmt.Errorf("gocraftWorkQueueInfo: unmarshal job: %w", err)
		}
		now := time.Now().Unix()
		info.Latency = uint64(now - job.EnqueuedAt)
	}

	return info, nil
}
