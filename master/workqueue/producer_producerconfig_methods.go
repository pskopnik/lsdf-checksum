package workqueue

func (p *ProducerConfig) CopyFrom(other *ProducerConfig) {
	p.MinWorkPackFileSize = other.MinWorkPackFileSize
	p.FetchRowBatchSize = other.FetchRowBatchSize
	p.RowBufferSize = other.RowBufferSize

	p.FileSystemName = other.FileSystemName
	p.Namespace = other.Namespace

	p.SnapshotName = other.SnapshotName

	p.Pool = other.Pool
	p.DB = other.DB
	p.Logger = other.Logger

	p.Controller = other.Controller
}

func (p *ProducerConfig) Merge(other *ProducerConfig) *ProducerConfig {
	if other.MinWorkPackFileSize != 0 {
		p.MinWorkPackFileSize = other.MinWorkPackFileSize
	}
	if other.FetchRowBatchSize != 0 {
		p.FetchRowBatchSize = other.FetchRowBatchSize
	}
	if other.RowBufferSize != 0 {
		p.RowBufferSize = other.RowBufferSize
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

	if other.Pool != nil {
		p.Pool = other.Pool
	}
	if other.DB != nil {
		p.DB = other.DB
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
