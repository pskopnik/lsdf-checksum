package workqueue

func (p *ProducerConfig) CopyFrom(other *ProducerConfig) {
	p.MinWorkPackFileSize = other.MinWorkPackFileSize
	p.MaxWorkPackFileNumber = other.MaxWorkPackFileNumber
	p.FetchRowChunkSize = other.FetchRowChunkSize
	p.FetchRowBatchSize = other.FetchRowBatchSize

	p.FileSystemName = other.FileSystemName
	p.Namespace = other.Namespace

	p.SnapshotName = other.SnapshotName

	p.DB = other.DB
	p.Queue = other.Queue
	p.Logger = other.Logger
	p.Controller = other.Controller
}

func (p *ProducerConfig) Merge(other *ProducerConfig) *ProducerConfig {
	if other.MinWorkPackFileSize != 0 {
		p.MinWorkPackFileSize = other.MinWorkPackFileSize
	}
	if other.MaxWorkPackFileNumber != 0 {
		p.MaxWorkPackFileNumber = other.MaxWorkPackFileNumber
	}
	if other.FetchRowChunkSize != 0 {
		p.FetchRowChunkSize = other.FetchRowChunkSize
	}
	if other.FetchRowBatchSize != 0 {
		p.FetchRowBatchSize = other.FetchRowBatchSize
	}

	if len(other.FileSystemName) > 0 {
		p.FileSystemName = other.FileSystemName
	}
	if len(other.Namespace) > 0 {
		p.Namespace = other.Namespace
	}

	if len(other.SnapshotName) > 0 {
		p.SnapshotName = other.SnapshotName
	}

	if other.DB != nil {
		p.DB = other.DB
	}
	if other.Queue != nil {
		p.Queue = other.Queue
	}
	if other.Logger != nil {
		p.Logger = other.Logger
	}
	if other.Controller != nil {
		p.Controller = other.Controller
	}

	return p
}

func (p *ProducerConfig) Clone() *ProducerConfig {
	config := &ProducerConfig{}
	config.CopyFrom(p)
	return config
}
