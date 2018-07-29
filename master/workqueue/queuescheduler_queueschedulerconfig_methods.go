package workqueue

func (q *QueueSchedulerConfig) CopyFrom(other *QueueSchedulerConfig) {
	q.Namespace = other.Namespace
	q.JobName = other.JobName

	q.Pool = other.Pool
	q.Logger = other.Logger

	q.Controller = other.Controller
}

func (q *QueueSchedulerConfig) Merge(other *QueueSchedulerConfig) *QueueSchedulerConfig {
	if len(other.Namespace) > 0 {
		q.Namespace = other.Namespace
	}
	if len(other.JobName) > 0 {
		q.JobName = other.JobName
	}

	if other.Pool != nil {
		q.Pool = other.Pool
	}
	if other.Logger != nil {
		q.Logger = other.Logger
	}

	if other.Controller != nil {
		q.Controller = other.Controller
	}

	return q
}

func (q *QueueSchedulerConfig) Clone() *QueueSchedulerConfig {
	config := &QueueSchedulerConfig{}
	config.CopyFrom(q)
	return config
}
