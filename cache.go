package pgtenant

import (
	"sync"

	"github.com/golang/groupcache/lru"
)

const maxCachedQueries = 1000

// queryCache implements a lru cache for dynamically
// transformed queries.
type queryCache struct {
	mu    sync.Mutex
	cache *lru.Cache // lazily initialized
}

func (qc *queryCache) lookup(q string) (tq Transformed, ok bool) {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	if qc.cache == nil {
		return tq, false
	}
	v, ok := qc.cache.Get(q)
	if ok {
		tq = v.(Transformed)
	}
	return tq, ok
}

func (qc *queryCache) add(q string, transformed Transformed) {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	if qc.cache == nil {
		qc.cache = lru.New(maxCachedQueries)
	}
	qc.cache.Add(q, transformed)
}
