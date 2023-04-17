package workqueue

import (
	"fmt"

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
		return nil, fmt.Errorf("Enqueuer.Enqueue: %w", err)
	}

	job, err := e.enqueuer.Enqueue(e.queueName, args)
	if err != nil {
		return nil, fmt.Errorf("Enqueuer.Enqueue: %w", err)
	}

	return job, nil
}
