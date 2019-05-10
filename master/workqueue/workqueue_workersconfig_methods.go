package workqueue

func (w *WorkersConfig) CopyFrom(other *WorkersConfig) {
	w.MaxFails = other.MaxFails
	w.PauseQueueLength = other.PauseQueueLength
	w.ResumeQueueLength = other.ResumeQueueLength
}

func (w *WorkersConfig) Merge(other *WorkersConfig) *WorkersConfig {
	if other.MaxFails != 0 {
		w.MaxFails = other.MaxFails
	}
	if other.PauseQueueLength != 0 {
		w.PauseQueueLength = other.PauseQueueLength
	}
	if other.ResumeQueueLength != 0 {
		w.ResumeQueueLength = other.ResumeQueueLength
	}

	return w
}

func (w *WorkersConfig) Clone() *WorkersConfig {
	config := &WorkersConfig{}
	config.CopyFrom(w)
	return config
}
