package worker

import (
	"testing"
)

const testKey = "someKey"
const testStr = "asdf"

func BenchmarkExpiringCache(b *testing.B) {
	count := 0

	cache := expiringCache{
		TTL: -1,
		Fetch: func(key interface{}) (interface{}, error) {
			count++
			return testStr, nil
		},
	}

	// b.SetParallelism(10)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = cache.Lookup(testKey)
		}
	})

	b.Logf("%d fetches requested", count)
}
