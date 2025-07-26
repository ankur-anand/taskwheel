package taskwheel_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/taskwheel"
)

func BenchmarkTimingWheel_AddRemove(b *testing.B) {
	tw := taskwheel.NewTimingWheel[any](10*time.Millisecond, 4096, 0)
	ids := make([]taskwheel.TimerID, b.N)
	for i := 0; i < b.N; i++ {
		ids[i] = taskwheel.TimerID(rune(i))
		_, _ = tw.AfterTimeout(ids[i], nil, 500*time.Millisecond)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tw.Remove(ids[i])
	}
}

func BenchmarkNativeTimer_AddRemove(b *testing.B) {
	timers := make([]*time.Timer, b.N)
	for i := 0; i < b.N; i++ {
		timers[i] = time.AfterFunc(time.Hour, func() {})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		timers[i].Stop()
	}
}

func BenchmarkTimingWheel_FireBatch(b *testing.B) {
	tw := taskwheel.NewTimingWheel[any](10*time.Millisecond, 4096, 0)
	const batch = 100000
	for i := 0; i < batch; i++ {
		_, _ = tw.AfterTimeout(taskwheel.TimerID(rune(i)), nil, 10*time.Millisecond)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tw.Tick()
	}
}

func BenchmarkTimingWheel_Memory(b *testing.B) {
	scales := []int{100_000, 1_000_000, 10_000_000}
	for _, n := range scales {
		b.Run(fmt.Sprintf("Timers_%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				tw := taskwheel.NewTimingWheel[any](10*time.Millisecond, 4096, 0)
				for j := 0; j < n; j++ {
					_, _ = tw.AfterTimeout(taskwheel.TimerID(rune(j)), nil, time.Hour)
				}
			}
		})
	}
}

func BenchmarkNativeTimer_Memory(b *testing.B) {
	scales := []int{100_000, 1_000_000, 10_000_000}
	for _, n := range scales {
		b.Run(fmt.Sprintf("Timers_%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				timers := make([]*time.Timer, n)
				for j := 0; j < n; j++ {
					timers[j] = time.AfterFunc(time.Hour, func() {})
				}
			}
		})
	}
}

const (
	numTimers          = 100_000
	benchmarkDuration  = 200 * time.Millisecond
	baseTickerInterval = 50 * time.Millisecond
)

func BenchmarkTimingWheelPeriodic(b *testing.B) {
	interval := 10 * time.Millisecond
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), benchmarkDuration)

		tw := taskwheel.NewTimingWheel[string](interval, 200, 0)

		for j := 0; j < numTimers; j++ {
			id := taskwheel.TimerID(strconv.Itoa(j))
			_, _ = tw.AfterTimeout(id, "payload", baseTickerInterval)
		}

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					due := tw.Tick()
					for _, timer := range due {
						_, _ = tw.AfterTimeout(timer.ID, timer.Value, baseTickerInterval)
					}
				case <-ctx.Done():
					return
				}
			}
		}()
		wg.Wait()
		cancel()
	}
}

func BenchmarkGoroutinePerTimer(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), benchmarkDuration)

		var wg sync.WaitGroup
		wg.Add(numTimers)

		for j := 0; j < numTimers; j++ {
			go func() {
				defer wg.Done()
				ticker := time.NewTicker(baseTickerInterval)
				defer ticker.Stop()
				for {
					select {
					case <-ticker.C:
					case <-ctx.Done():
						return
					}
				}
			}()
		}
		wg.Wait()
		cancel()
	}
}
