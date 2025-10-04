package cache

import (
	"sync"
	"time"

	"github.com/ankur-anand/taskwheel"
)

type Entry struct {
	Key      string
	Value    interface{}
	ExpireAt time.Time
}

type OptimizedTimingWheelCache struct {
	wheel       *taskwheel.HierarchicalTimingWheel[string]
	cache       map[string]*Entry
	mu          sync.RWMutex
	expirations int64
}

func NewOptimizedTimingWheelCache() *OptimizedTimingWheelCache {
	twc := &OptimizedTimingWheelCache{
		wheel: taskwheel.NewHierarchicalTimingWheel[string](
			[]time.Duration{1 * time.Second, 1 * time.Minute, 1 * time.Hour},
			[]int{60, 60, 24},
		),
		cache: make(map[string]*Entry),
	}

	// expiration handler
	twc.wheel.StartBatch(1*time.Second, func(timers []*taskwheel.Timer[string]) {
		for _, t := range timers {
			twc.onExpire(t.Value)
		}
	})

	return twc
}

func (twc *OptimizedTimingWheelCache) Set(key string, value interface{}, ttl time.Duration) {
	entry := &Entry{
		Key:      key,
		Value:    value,
		ExpireAt: time.Now().Add(ttl),
	}

	twc.mu.Lock()
	twc.cache[key] = entry
	twc.mu.Unlock()

	twc.wheel.AfterTimeout(taskwheel.HashID(key), key, ttl)
}

func (twc *OptimizedTimingWheelCache) Get(key string) (interface{}, bool) {
	twc.mu.RLock()
	defer twc.mu.RUnlock()

	entry, exists := twc.cache[key]
	if !exists {
		return nil, false
	}

	return entry.Value, true
}

func (twc *OptimizedTimingWheelCache) onExpire(key string) {
	twc.mu.Lock()
	delete(twc.cache, key)
	twc.expirations++
	twc.mu.Unlock()
}

func (twc *OptimizedTimingWheelCache) Size() int {
	twc.mu.RLock()
	defer twc.mu.RUnlock()
	return len(twc.cache)
}

func (twc *OptimizedTimingWheelCache) GetMetrics() int64 {
	twc.mu.RLock()
	defer twc.mu.RUnlock()
	return twc.expirations
}
