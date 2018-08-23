package cache

import (
	"sync"
	"time"
)

type cacheEntry struct {
	Exists  bool
	Lock    sync.RWMutex
	Written time.Time
	Value   interface{}
}

// ExpiringCache provides a cache with map-style access where each entry has a
// maximum lifetime.
// After an entry expired, it has to be re-fetched.
//
// Create an ExpiringCache by declaring a new struct and setting its Fetch and
// optionally its TTL fields.
// If TTL is 0, keys will be fetched on every Lookup().
// If TTL is negative, entries will have an infinite lifetime and will never be
// re-fetched.
//
// When an entry expired, it is still allocated in the map. Use RemoveExpired()
// to remove expired entries.
type ExpiringCache struct {
	cache sync.Map
	TTL   time.Duration
	Fetch func(key interface{}) (interface{}, error)
}

func (e *ExpiringCache) Lookup(key interface{}) (interface{}, error) {
	now := time.Now()

	value, ok, entry := e.loadValueAndEntry(key, now)
	if ok {
		return value, nil
	}

	if entry == nil {
		value, ok, entry = e.allocateAndStoreLockedEntry(key, now)
		if ok {
			return value, nil
		}
	} else {
		// Lock entry returned by loadValueAndEntry()
		entry.Lock.Lock()
	}

	if e.isEntryValid(entry, now) {
		value = entry.Value
		entry.Lock.Unlock()

		return value, nil
	}

	value, err := e.Fetch(key)
	if err != nil {
		entry.Lock.Unlock()
		return nil, err
	}

	entry.Exists = true
	entry.Written = now
	entry.Value = value

	entry.Lock.Unlock()

	return value, nil
}

func (e *ExpiringCache) loadValueAndEntry(
	key interface{}, now time.Time,
) (interface{}, bool, *cacheEntry) {
	var entry *cacheEntry

	entryIntf, ok := e.cache.Load(key)
	if ok {
		entry = entryIntf.(*cacheEntry)

		entry.Lock.RLock()

		if e.isEntryValid(entry, now) {
			value := entry.Value
			entry.Lock.RUnlock()

			return value, true, nil
		} else {
			entry.Lock.RUnlock()

		}
	}

	return nil, false, entry
}

func (e *ExpiringCache) allocateAndStoreLockedEntry(
	key interface{}, now time.Time,
) (interface{}, bool, *cacheEntry) {
	entry := &cacheEntry{}

	entry.Lock.Lock()

	loadedEntryIntf, loaded := e.cache.LoadOrStore(key, entry)
	if loaded {
		// Unlock entry to be discarded... just for form
		entry.Lock.Unlock()

		entry = loadedEntryIntf.(*cacheEntry)

		entry.Lock.RLock()

		if e.isEntryValid(entry, now) {
			value := entry.Value
			entry.Lock.RUnlock()

			return value, true, nil
		} else {
			entry.Lock.RUnlock()

			entry.Lock.Lock()
		}
	}

	return nil, false, entry
}

func (e *ExpiringCache) isEntryValid(entry *cacheEntry, now time.Time) bool {
	if now.IsZero() {
		now = time.Now()
	}
	return entry.Exists && e.TTL < 0 || entry.Written.Add(e.TTL).After(now)
}

func (e *ExpiringCache) RemoveExpired() uint {
	var removed uint
	var toBeDeleted bool

	if e.TTL < 0 {
		return removed
	}

	now := time.Now()

	e.cache.Range(func(key interface{}, value interface{}) bool {
		entry := value.(*cacheEntry)

		entry.Lock.RLock()

		toBeDeleted = entry.Written.Add(e.TTL).Before(now)

		entry.Lock.RUnlock()

		if toBeDeleted {
			entry.Lock.Lock()

			// Check again
			if !entry.Written.Add(e.TTL).Before(now) {
				entry.Lock.Unlock()
				return true
			}

			entry.Exists = false
			e.cache.Delete(key)
			removed++

			entry.Lock.Unlock()
		}

		return true
	})

	return removed
}
