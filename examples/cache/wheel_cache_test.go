//go:build examples

package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestOptimizedWheelPerformance(t *testing.T) {
	sizes := []int{10000, 100000, 1000000, 10000000}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("Size_%d", size), func(t *testing.T) {
			cache := NewOptimizedTimingWheelCache()

			t.Logf("Populating %d entries...", size)
			startPopulate := time.Now()
			for i := 0; i < size; i++ {
				ttl := time.Duration(30+i%30) * time.Second
				cache.Set(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i), ttl)
			}
			populateTime := time.Since(startPopulate)

			t.Logf("Populate time: %v", populateTime)
			t.Logf("Cache size: %d", cache.Size())

			t.Logf("Running for 10 seconds...")
			time.Sleep(10 * time.Second)

			expired := cache.GetMetrics()

			t.Logf("\nResults:")
			t.Logf("  Populate time: %v", populateTime)
			t.Logf("  Expirations: %d", expired)
			t.Logf("  Final cache size: %d", cache.Size())
		})
	}
}

func TestOptimizedWheelWithConcurrentReads(t *testing.T) {

	sizes := []int{100000, 1000000, 10000000}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("Size_%d", size), func(t *testing.T) {
			cache := NewOptimizedTimingWheelCache()

			t.Logf("Populating %d entries...", size)
			for i := 0; i < size; i++ {
				ttl := time.Duration(30+i%30) * time.Second
				cache.Set(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i), ttl)
			}

			var readLatencies []time.Duration
			var mu sync.Mutex
			done := make(chan struct{})

			// concurrent readers
			for i := 0; i < 10; i++ {
				go func(id int) {
					for {
						select {
						case <-done:
							return
						default:
							start := time.Now()
							cache.Get(fmt.Sprintf("key-%d", id*1000))
							latency := time.Since(start)

							mu.Lock()
							readLatencies = append(readLatencies, latency)
							mu.Unlock()

							time.Sleep(10 * time.Millisecond)
						}
					}
				}(i)
			}

			time.Sleep(10 * time.Second)

			close(done)
			time.Sleep(100 * time.Millisecond)

			mu.Lock()
			var totalLatency time.Duration
			var maxLatency time.Duration
			for _, lat := range readLatencies {
				totalLatency += lat
				if lat > maxLatency {
					maxLatency = lat
				}
			}
			avgLatency := time.Duration(0)
			if len(readLatencies) > 0 {
				avgLatency = totalLatency / time.Duration(len(readLatencies))
			}
			mu.Unlock()

			expired := cache.GetMetrics()

			t.Logf("\nConcurrent Read Performance:")
			t.Logf("  Total reads: %d", len(readLatencies))
			t.Logf("  Avg read latency: %v", avgLatency)
			t.Logf("  Max read latency: %v", maxLatency)
			t.Logf("  Expirations: %d", expired)
		})
	}
}

func TestOptimizedWheelScaling(t *testing.T) {
	t.Log("\n=== Timing Wheel Scaling Analysis ===\n")

	sizes := []int{1000, 10000, 100000, 1000000, 10000000}
	results := make(map[int]time.Duration)

	for _, size := range sizes {
		cache := NewOptimizedTimingWheelCache()

		start := time.Now()
		for i := 0; i < size; i++ {
			ttl := time.Duration(30+i%30) * time.Second
			cache.Set(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i), ttl)
		}
		populateTime := time.Since(start)
		results[size] = populateTime

		t.Logf("@ %d entries: populate in %v", size, populateTime)
	}

	t.Logf("\nScaling Analysis:")
	prevSize := 1000
	prevTime := results[1000]

	for _, size := range sizes[1:] {
		scaleFactor := float64(size) / float64(prevSize)
		timeRatio := float64(results[size]) / float64(prevTime)

		t.Logf("  %dK → %dK (%.0fx scale): %.2fms → %.2fms (%.2fx time)",
			prevSize/1000, size/1000, scaleFactor,
			float64(prevTime.Microseconds())/1000.0,
			float64(results[size].Microseconds())/1000.0,
			timeRatio)

		prevSize = size
		prevTime = results[size]
	}
}

func TestScanVsWheelComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping comparison test in short mode")
	}

	sizes := []int{100000, 1000000, 10000000}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("Size_%d", size), func(t *testing.T) {
			t.Logf("\n=== Direct Comparison @ %d entries ===\n", size)

			t.Run("PeriodicScan", func(t *testing.T) {
				cache := NewSimpleScanCache()

				start := time.Now()
				for i := 0; i < size; i++ {
					ttl := time.Duration(30+i%30) * time.Second
					cache.Set(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i), ttl)
				}
				populateTime := time.Since(start)

				time.Sleep(2 * time.Second)
				_, scanTime := cache.scanAndDelete()

				cpuPercent := float64(scanTime.Milliseconds()) / 1000.0 * 100

				t.Logf("Periodic Scan Results:")
				t.Logf("  Populate: %v", populateTime)
				t.Logf("  Scan time: %v", scanTime)
				t.Logf("  CPU: %.2f%%", cpuPercent)
			})

			t.Run("TimingWheel", func(t *testing.T) {
				cache := NewOptimizedTimingWheelCache()

				start := time.Now()
				for i := 0; i < size; i++ {
					ttl := time.Duration(30+i%30) * time.Second
					cache.Set(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i), ttl)
				}
				populateTime := time.Since(start)

				time.Sleep(10 * time.Second)
				exp := cache.GetMetrics()

				t.Logf("Timing Wheel Results:")
				t.Logf("  Populate: %v", populateTime)
				t.Logf("  Expirations: %d", exp)
			})
		})
	}
}

func BenchmarkOptimizedWheelOperations(b *testing.B) {
	sizes := []int{10000, 100000, 1000000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Set_%d", size), func(b *testing.B) {
			cache := NewOptimizedTimingWheelCache()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("key-%d", i%size)
				cache.Set(key, "value", 30*time.Second)
			}
		})

		b.Run(fmt.Sprintf("Get_%d", size), func(b *testing.B) {
			cache := NewOptimizedTimingWheelCache()

			for i := 0; i < size; i++ {
				cache.Set(fmt.Sprintf("key-%d", i), "value", 30*time.Second)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("key-%d", i%size)
				cache.Get(key)
			}
		})
	}
}
