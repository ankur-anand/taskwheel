package taskwheel

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkNativeTimers measures performance using standard Go time.AfterFunc
func BenchmarkNativeTimers(b *testing.B) {
	scenarios := []struct {
		name      string
		numTimers int
		timeout   time.Duration
	}{
		{"1K_timers_100ms", 1000, 100 * time.Millisecond},
		{"10K_timers_100ms", 10000, 100 * time.Millisecond},
		{"100K_timers_1s", 100000, 1 * time.Second},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				var wg sync.WaitGroup
				wg.Add(scenario.numTimers)
				var fired atomic.Int64

				b.StartTimer()
				for j := 0; j < scenario.numTimers; j++ {
					time.AfterFunc(scenario.timeout, func() {
						fired.Add(1)
						wg.Done()
					})
				}
				wg.Wait()
				b.StopTimer()

				if fired.Load() != int64(scenario.numTimers) {
					b.Fatalf("Expected %d timers to fire, got %d", scenario.numTimers, fired.Load())
				}
			}
		})
	}
}

// BenchmarkTimingWheelAfterTimeout measures performance using HierarchicalTimingWheel
func BenchmarkTimingWheelAfterTimeout(b *testing.B) {
	scenarios := []struct {
		name         string
		numTimers    int
		timeout      time.Duration
		tickInterval time.Duration
	}{
		{"1K_timers_100ms", 1000, 100 * time.Millisecond, 10 * time.Millisecond},
		{"10K_timers_100ms", 10000, 100 * time.Millisecond, 10 * time.Millisecond},
		{"100K_timers_100ms", 100000, 100 * time.Millisecond, 10 * time.Millisecond}, // Changed to 100ms for fair comparison
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			b.ReportAllocs()

			intervals := []time.Duration{10 * time.Millisecond, 100 * time.Millisecond, 1 * time.Second}
			slots := []int{10, 100, 60}
			htw := NewHierarchicalTimingWheel[int](intervals, slots)

			var wg sync.WaitGroup
			var fired atomic.Int64

			stop := htw.StartBatch(scenario.tickInterval, func(timers []*Timer[int]) {
				for range timers {
					fired.Add(1)
					wg.Done()
				}
			})
			defer stop()

			ids := make([]TimerID, scenario.numTimers)
			for j := 0; j < scenario.numTimers; j++ {
				ids[j] = TimerID(fmt.Sprintf("timer-%d", j))
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				wg.Add(scenario.numTimers)
				fired.Store(0)

				for j := 0; j < scenario.numTimers; j++ {
					_, err := htw.AfterTimeout(ids[j], j, scenario.timeout)
					if err != nil {
						b.Fatal(err)
					}
				}

				wg.Wait()

				if fired.Load() != int64(scenario.numTimers) {
					b.Fatalf("Expected %d timers to fire, got %d", scenario.numTimers, fired.Load())
				}

				htw.Reset()
			}
		})
	}
}

// BenchmarkMemoryComparison compares memory usage
func BenchmarkMemoryComparison(b *testing.B) {
	b.Run("Native_10K_timers", func(b *testing.B) {
		b.ReportAllocs()
		timeout := 100 * time.Millisecond

		for i := 0; i < b.N; i++ {
			var wg sync.WaitGroup
			wg.Add(10000)

			for j := 0; j < 10000; j++ {
				time.AfterFunc(timeout, func() {
					wg.Done()
				})
			}
			wg.Wait()
		}
	})

	b.Run("TimingWheel_10K_timers", func(b *testing.B) {
		b.ReportAllocs()
		timeout := 100 * time.Millisecond

		intervals := []time.Duration{10 * time.Millisecond, 100 * time.Millisecond, 1 * time.Second}
		slots := []int{10, 100, 60}
		htw := NewHierarchicalTimingWheel[int](intervals, slots)

		var wg sync.WaitGroup

		stop := htw.StartBatch(10*time.Millisecond, func(timers []*Timer[int]) {
			for range timers {
				wg.Done()
			}
		})
		defer stop()

		ids := make([]TimerID, 10000)
		for j := 0; j < 10000; j++ {
			ids[j] = TimerID(fmt.Sprintf("timer-%d", j))
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			wg.Add(10000)

			for j := 0; j < 10000; j++ {
				_, err := htw.AfterTimeout(ids[j], j, timeout)
				if err != nil {
					b.Fatal(err)
				}
			}

			wg.Wait()
			htw.Reset()
		}
	})
}
