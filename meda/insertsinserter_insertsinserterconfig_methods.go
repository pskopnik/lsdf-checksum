package meda

func (c *InsertsInserterConfig) CopyFrom(other *InsertsInserterConfig) {
	c.RowsPerStmt = other.RowsPerStmt
	c.StmtsPerTransaction = other.StmtsPerTransaction
	c.Concurrency = other.Concurrency
	c.MaxWaitTime = other.MaxWaitTime
	c.BatchQueueSize = other.BatchQueueSize
}

func (c *InsertsInserterConfig) Merge(other *InsertsInserterConfig) *InsertsInserterConfig {
	if other.RowsPerStmt != 0 {
		c.RowsPerStmt = other.RowsPerStmt
	}

	if other.StmtsPerTransaction != 0 {
		c.StmtsPerTransaction = other.StmtsPerTransaction
	}

	if other.Concurrency != 0 {
		c.Concurrency = other.Concurrency
	}

	if other.MaxWaitTime != 0 {
		c.MaxWaitTime = other.MaxWaitTime
	}

	if other.BatchQueueSize != 0 {
		c.BatchQueueSize = other.BatchQueueSize
	}

	return c
}

func (c *InsertsInserterConfig) Clone() *InsertsInserterConfig {
	config := &InsertsInserterConfig{}
	config.CopyFrom(c)
	return config
}
