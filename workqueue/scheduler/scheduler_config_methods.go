package scheduler

func (q *Config) CopyFrom(other *Config) {
	q.Namespace = other.Namespace
	q.JobName = other.JobName

	q.Pool = other.Pool
	q.Logger = other.Logger

	q.Controller = other.Controller
}

func (q *Config) Merge(other *Config) *Config {
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

func (q *Config) Clone() *Config {
	config := &Config{}
	config.CopyFrom(q)
	return config
}
