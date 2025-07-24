package taskwheel_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/ankur-anand/taskwheel"
)

func BenchmarkTimingWheel_AddRemove(b *testing.B) {
	tw := taskwheel.NewTimingWheel[any](10*time.Millisecond, 4096)
	ids := make([]taskwheel.TimerID, b.N)
	for i := 0; i < b.N; i++ {
		ids[i] = taskwheel.TimerID(rune(i))
		tw.AfterTimeout(ids[i], nil, 500*time.Millisecond)
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
	tw := taskwheel.NewTimingWheel[any](10*time.Millisecond, 4096)
	const batch = 100000
	for i := 0; i < batch; i++ {
		tw.AfterTimeout(taskwheel.TimerID(rune(i)), nil, 10*time.Millisecond)
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
				tw := taskwheel.NewTimingWheel[any](10*time.Millisecond, 4096)
				for j := 0; j < n; j++ {
					tw.AfterTimeout(taskwheel.TimerID(rune(j)), nil, time.Hour)
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
