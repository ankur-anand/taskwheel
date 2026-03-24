package taskwheel_test

import (
	"testing"
	"time"

	"github.com/ankur-anand/taskwheel"
)

var hierarchicalBenchTimeouts = []time.Duration{
	20 * time.Millisecond,
	350 * time.Millisecond,
	5 * time.Second,
	2 * time.Minute,
}

func newBenchmarkHierarchicalWheel() *taskwheel.HierarchicalTimingWheel[any] {
	return taskwheel.NewHierarchicalTimingWheel[any](
		[]time.Duration{10 * time.Millisecond, 100 * time.Millisecond, time.Second, time.Minute},
		[]int{10, 10, 60, 60},
	)
}

func benchmarkHierarchicalIDs(b *testing.B, wheel *taskwheel.HierarchicalTimingWheel[any], keyspace int) []taskwheel.TimerID {
	b.Helper()

	ids := make([]taskwheel.TimerID, keyspace)
	for i := range ids {
		ids[i] = taskwheel.TimerID(i + 1)
		_, err := wheel.AfterTimeout(ids[i], nil, hierarchicalBenchTimeouts[i%len(hierarchicalBenchTimeouts)])
		if err != nil {
			b.Fatalf("seed timer %d: %v", i, err)
		}
	}
	return ids
}

func BenchmarkHierarchicalTimingWheel_RescheduleExisting(b *testing.B) {
	const keyspace = 1 << 15

	wheel := newBenchmarkHierarchicalWheel()
	ids := benchmarkHierarchicalIDs(b, wheel, keyspace)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := ids[i%keyspace]
		timeout := hierarchicalBenchTimeouts[i%len(hierarchicalBenchTimeouts)]
		if _, err := wheel.AfterTimeout(id, nil, timeout); err != nil {
			b.Fatalf("reschedule timer %d: %v", i, err)
		}
	}
}

func BenchmarkHierarchicalTimingWheel_GetExisting(b *testing.B) {
	const keyspace = 1 << 15

	wheel := newBenchmarkHierarchicalWheel()
	ids := benchmarkHierarchicalIDs(b, wheel, keyspace)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := wheel.Get(ids[i%keyspace]); !ok {
			b.Fatalf("missing timer %d", i)
		}
	}
}
