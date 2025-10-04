package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestScanPerformance(t *testing.T) {
	sizes := []int{10000, 100000, 1000000, 10000000}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("Size_%d", size), func(t *testing.T) {
			cache := NewSimpleScanCache()

			t.Logf("Populating %d entries...", size)
			for i := 0; i < size; i++ {
				ttl := time.Duration(30+i%30) * time.Second
				cache.Set(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i), ttl)
			}

			t.Logf("Cache size: %d", cache.Size())

			var totalScanTime time.Duration
			var totalDeleted int
			numScans := 10

			t.Logf("Running %d scans...", numScans)
			for i := 0; i < numScans; i++ {
				time.Sleep(1 * time.Second)

				deleted, scanTime := cache.scanAndDelete()
				totalScanTime += scanTime
				totalDeleted += deleted

				if i < 3 || i == numScans-1 {
					t.Logf("  Scan #%d: %v (%d deleted, %d remaining)",
						i+1, scanTime, deleted, cache.Size())
				}
			}

			avgScanTime := totalScanTime / time.Duration(numScans)
			cpuPercent := float64(avgScanTime.Milliseconds()) / 1000.0 * 100

			t.Logf("\nResults:")
			t.Logf("  Total scans: %d", numScans)
			t.Logf("  Total deleted: %d", totalDeleted)
			t.Logf("  Avg scan time: %v", avgScanTime)
			t.Logf("  CPU overhead: %.2f%%", cpuPercent)
			t.Logf("  Final cache size: %d", cache.Size())
		})
	}
}

func BenchmarkScanOperation(b *testing.B) {
	sizes := []int{10000, 100000, 1000000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			cache := NewSimpleScanCache()

			for i := 0; i < size; i++ {
				ttl := time.Duration(30+i%30) * time.Second
				cache.Set(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i), ttl)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				cache.scanAndDelete()
			}
		})
	}
}

func TestScanWithConcurrentReads(t *testing.T) {

	sizes := []int{100000, 1000000, 10000000}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("Size_%d", size), func(t *testing.T) {
			cache := NewSimpleScanCache()

			t.Logf("Populating %d entries...", size)
			for i := 0; i < size; i++ {
				ttl := time.Duration(30+i%30) * time.Second
				cache.Set(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i), ttl)
			}

			var readLatencies []time.Duration
			var mu sync.Mutex
			done := make(chan struct{})

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

			time.Sleep(500 * time.Millisecond)

			t.Logf("Starting scans with concurrent reads...")
			var scanTimes []time.Duration

			for i := 0; i < 10; i++ {
				_, scanTime := cache.scanAndDelete()
				scanTimes = append(scanTimes, scanTime)
				t.Logf("  Scan #%d: %v", i+1, scanTime)
				time.Sleep(1 * time.Second)
			}

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
			avgLatency := totalLatency / time.Duration(len(readLatencies))
			mu.Unlock()

			var totalScan time.Duration
			for _, st := range scanTimes {
				totalScan += st
			}
			avgScan := totalScan / time.Duration(len(scanTimes))

			t.Logf("\nConcurrent Read Impact:")
			t.Logf("  Total reads: %d", len(readLatencies))
			t.Logf("  Avg read latency: %v", avgLatency)
			t.Logf("  Max read latency: %v (blocked by scan!)", maxLatency)
			t.Logf("  Avg scan time: %v", avgScan)
			t.Logf("  Scan blocks reads for: %v", avgScan)
		})
	}
}

func TestScanScaling(t *testing.T) {
	t.Log("\n=== Scan Time Scaling Analysis ===\n")

	sizes := []int{1000, 10000, 100000, 1000000, 10000000}
	results := make(map[int]time.Duration)

	for _, size := range sizes {
		cache := NewSimpleScanCache()

		for i := 0; i < size; i++ {
			ttl := time.Duration(30+i%30) * time.Second
			cache.Set(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i), ttl)
		}

		time.Sleep(2 * time.Second)

		_, scanTime := cache.scanAndDelete()
		results[size] = scanTime

		t.Logf("@ %d entries: %v", size, scanTime)
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

func BenchmarkScanVsMapSize(b *testing.B) {
	sizes := []int{1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Scan_%d", size), func(b *testing.B) {
			cache := NewSimpleScanCache()

			for i := 0; i < size; i++ {
				cache.Set(fmt.Sprintf("key-%d", i), "value", 30*time.Second)
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				cache.scanAndDelete()
			}

			b.ReportMetric(float64(size), "entries")
		})
	}
}
