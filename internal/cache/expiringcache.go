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
//
// TTL may be changed at any point. Access to the field is not protected in
// any way. Thus, care must be taken that there are no ongoing operations on
// the ExpiringCache.
//
// When an entry expired, it is still allocated in the map. Use RemoveExpired()
// to remove expired entries.
type ExpiringCache struct {
	cache sync.Map
	// TTL is the duration the value of an entry is considered valid for. When
	// retrieving an invalid entry, the value is re-fetched. The
	// implementation ensures that the value is considered valid for at most
	// TTL.
	//
	// If TTL is 0, keys will be fetched on every Lookup().
	// If TTL is negative, entries will have an infinite lifetime and will never be
	// re-fetched by Lookup().
	TTL time.Duration
	// Fetch is called to populate the value for the passed in key. Any error
	// returned is passed back to the caller of the ExpiringCache method.
	Fetch func(key interface{}) (interface{}, error)
}

// Lookup returns the value for the key passed to the method.
//
// Lookup respects the cache and the TTL value set on the ExpiringCache. A
// Fetch() call is only issued when the key is not in the cache or the value
// was written more than TTL ago. Lookup stores the value returned by Fetch()
// in the cache. Lookup never issues concurrent Fetch() calls for the same
// key.
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

// ForceFetch returns the value for the key passed to the method by issuing a
// call to Fetch().
//
// ForceFetch respects the locking mechanisms of the cache but ignores the
// TTL. ForceFetch stores the value returned by Fetch() in the cache.
// ForceFetch never issues concurrent Fetch() calls for the same key.
func (e *ExpiringCache) ForceFetch(key interface{}) (interface{}, error) {
	now := time.Now()

	_, _, entry := e.loadValueAndEntry(key, now)

	if entry == nil {
		_, _, entry = e.allocateAndStoreLockedEntry(key, now)
	} else {
		// Lock entry returned by loadValueAndEntry()
		entry.Lock.Lock()
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

// RemoveExpired iterates over all keys in the cache and removes all entries
// which are no longer valid. This leads to eventual de-allocation of the
// entries.
//
// RemoveExpired returns the number of entries which have been removed.
func (e *ExpiringCache) RemoveExpired() uint {
	var removed uint

	if e.TTL < 0 {
		return removed
	}

	now := time.Now()

	e.cache.Range(func(key interface{}, value interface{}) bool {
		entry := value.(*cacheEntry)

		entry.Lock.RLock()

		toBeDeleted := entry.Written.Add(e.TTL).Before(now)

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

// Clear removes all entries from the cache. This leads to eventual
// de-allocation of the entries.
//
// Clear returns the number of entries which have been removed.
func (e *ExpiringCache) Clear() uint {
	var removed uint

	e.cache.Range(func(key interface{}, value interface{}) bool {
		entry := value.(*cacheEntry)

		entry.Lock.Lock()

		entry.Exists = false
		e.cache.Delete(key)
		removed++

		entry.Lock.Unlock()

		return true
	})

	return removed
}
