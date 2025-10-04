package cache

import (
	"sync"
	"time"
)

type SimpleScanCache struct {
	data map[string]*ScanEntry
	mu   sync.RWMutex
}

type ScanEntry struct {
	Value     interface{}
	ExpiresAt time.Time
}

func NewSimpleScanCache() *SimpleScanCache {
	return &SimpleScanCache{
		data: make(map[string]*ScanEntry),
	}
}

func (c *SimpleScanCache) Set(key string, value interface{}, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data[key] = &ScanEntry{
		Value:     value,
		ExpiresAt: time.Now().Add(ttl),
	}
}

func (c *SimpleScanCache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.data[key]
	if !exists {
		return nil, false
	}

	return entry.Value, true
}

func (c *SimpleScanCache) scanAndDelete() (int, time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	start := time.Now()
	now := time.Now()
	deleted := 0

	for key, entry := range c.data {
		if now.After(entry.ExpiresAt) {
			delete(c.data, key)
			deleted++
		}
	}

	return deleted, time.Since(start)
}

func (c *SimpleScanCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.data)
}
