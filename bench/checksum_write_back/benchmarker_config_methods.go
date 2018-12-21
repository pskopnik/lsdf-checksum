package main

func (b *BenchmarkerConfig) CopyFrom(other *BenchmarkerConfig) {
	b.DB = other.DB
	b.Logger = other.Logger
	b.GenerateAndWriteVariation.BatchSize = other.GenerateAndWriteVariation.BatchSize
	b.GenerateAndWriteVariation.TransactionSize = other.GenerateAndWriteVariation.TransactionSize
}

func (b *BenchmarkerConfig) Merge(other *BenchmarkerConfig) *BenchmarkerConfig {
	if other.DB != nil {
		b.DB = other.DB
	}
	if other.Logger != nil {
		b.Logger = other.Logger
	}
	if other.GenerateAndWriteVariation.BatchSize != 0 {
		b.GenerateAndWriteVariation.BatchSize = other.GenerateAndWriteVariation.BatchSize
	}
	if other.GenerateAndWriteVariation.TransactionSize != 0 {
		b.GenerateAndWriteVariation.TransactionSize = other.GenerateAndWriteVariation.TransactionSize
	}

	return b
}

func (b *BenchmarkerConfig) Clone() *BenchmarkerConfig {
	config := &BenchmarkerConfig{}
	config.CopyFrom(b)
	return config
}
