package main

import (
	"testing"
)

func BenchmarkPropagateJob(b *testing.B) {
	jobArgs := make(map[string]interface{})
	for i := 0; i < b.N; i++ {
		propagateJob(jobArgs, &sampleWorkPack)
	}
}

func BenchmarkRetrievePack(b *testing.B) {
	jobArgs := make(map[string]interface{})
	propagateJob(jobArgs, &sampleWorkPack)

	for i := 0; i < b.N; i++ {
		retrievePack(jobArgs)
	}
}
