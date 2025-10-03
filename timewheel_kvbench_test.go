package taskwheel

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type SimpleTimingWheelCache struct {
	wheel *TimingWheel[string]
	mu    sync.RWMutex
	data  map[string]string
}

func NewSimpleTimingWheelCache(tickInterval time.Duration, slots int) *SimpleTimingWheelCache {
	return &SimpleTimingWheelCache{
		wheel: NewTimingWheel[string](tickInterval, slots, 0),
		data:  make(map[string]string),
	}
}

func (c *SimpleTimingWheelCache) Set(key, value string, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = value
	_, _ = c.wheel.AfterTimeout(HashID(key), key, ttl)
}

func (c *SimpleTimingWheelCache) Get(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	value, ok := c.data[key]
	return value, ok
}

func (c *SimpleTimingWheelCache) ManualTick() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	expired := c.wheel.Tick()
	for _, timer := range expired {
		delete(c.data, timer.Value)
	}
	return len(expired)
}

func (c *SimpleTimingWheelCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.data)
}

type NativeTimerCache struct {
	mu     sync.RWMutex
	data   map[string]string
	timers map[string]*time.Timer
}

func NewNativeTimerCache() *NativeTimerCache {
	return &NativeTimerCache{
		data:   make(map[string]string),
		timers: make(map[string]*time.Timer),
	}
}

func (c *NativeTimerCache) Set(key, value string, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if timer, exists := c.timers[key]; exists {
		timer.Stop()
	}

	c.data[key] = value
	c.timers[key] = time.AfterFunc(ttl, func() {
		c.mu.Lock()
		delete(c.data, key)
		delete(c.timers, key)
		c.mu.Unlock()
	})
}

func (c *NativeTimerCache) Get(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	value, ok := c.data[key]
	return value, ok
}

func (c *NativeTimerCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if timer, exists := c.timers[key]; exists {
		timer.Stop()
		delete(c.timers, key)
	}
	delete(c.data, key)
}

func (c *NativeTimerCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.data)
}

func BenchmarkExpirationOverhead_N(b *testing.B) {
	numKeys := []int{1_000, 10_000, 100_000}

	for _, n := range numKeys {
		b.Run(fmt.Sprintf("TimingWheel_Expiration_%d", n), func(b *testing.B) {

			keys := make([]string, n)
			for i := 0; i < n; i++ {
				keys[i] = fmt.Sprintf("key-%d", i)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				cache := NewSimpleTimingWheelCache(1*time.Millisecond, 10000)

				for j := 0; j < n; j++ {
					cache.Set(keys[j], "value", 50*time.Millisecond)
				}

				b.StartTimer()
				start := time.Now()

				time.Sleep(50 * time.Millisecond)

				totalExpired := 0
				tickCount := 0
				for totalExpired < n {
					expired := cache.ManualTick()
					totalExpired += expired
					tickCount++

					if tickCount > 10000 {
						b.Fatalf("Too many ticks, possible infinite loop")
					}
				}

				duration := time.Since(start)

				if i == 0 {
					b.Logf("Expired %d timers in %v (%.2f ns/timer, %d ticks)",
						n, duration, float64(duration.Nanoseconds())/float64(n), tickCount)
				}
			}
		})

		b.Run(fmt.Sprintf("NativeTimer_Expiration_%d", n), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				var expiredCount int64
				var wg sync.WaitGroup
				wg.Add(n)

				start := time.Now()

				// this is very similar we are not making any map here as of now.
				for j := 0; j < n; j++ {
					time.AfterFunc(50*time.Millisecond, func() {
						atomic.AddInt64(&expiredCount, 1)
						wg.Done()
					})
				}

				wg.Wait()

				duration := time.Since(start)
				expired := atomic.LoadInt64(&expiredCount)

				if i == 0 {
					b.Logf("Expired %d timers in %v (%.2f ns/timer)",
						expired, duration, float64(duration.Nanoseconds())/float64(expired))
				}
			}
		})
	}
}
