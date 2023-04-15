package workqueue

import (
	"github.com/gocraft/work"
)

type Enqueuer[T JobPayload] struct {
	w         *Workqueue
	enqueuer  *work.Enqueuer
	queueName string
}

func (e *Enqueuer[T]) QueueName() string {
	return e.queueName
}

func (e *Enqueuer[T]) Enqueue(payload T) (*work.Job, error) {
	args := make(map[string]interface{})

	err := payload.ToJobArgs(args)
	if err != nil {
		return nil, err
	}

	job, err := e.enqueuer.Enqueue(e.queueName, args)
	if err != nil {
		return nil, err
	}

	return job, nil
}
