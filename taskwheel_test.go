package taskwheel_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/taskwheel"
)

func TestRescheduler_DuplicateIDHandling(t *testing.T) {
	intervals := []time.Duration{1 * time.Millisecond, 10 * time.Millisecond}
	slots := []int{100, 100}
	wheel := taskwheel.NewHierarchicalTimingWheel[string](intervals, slots)

	var fired atomic.Int64
	stop := wheel.StartBatch(1*time.Millisecond, func(ts []*taskwheel.Timer[string]) {
		for range ts {
			fired.Add(1)
		}
	})
	defer stop()

	id := taskwheel.HashID("test-timer")

	if _, err := wheel.AfterTimeout(id, "first", 50*time.Millisecond); err != nil {
		t.Fatalf("first add failed: %v", err)
	}
	if _, err := wheel.AfterTimeout(id, "second", 50*time.Millisecond); err != nil {
		t.Fatalf("second add failed: %v", err)
	}

	time.Sleep(120 * time.Millisecond)

	if got := fired.Load(); got != 1 {
		t.Fatalf("expected 1 fire (replacement), got %d", got)
	}

	if exists := wheel.Remove(id); exists {
		t.Fatalf("timer should not exist after firing")
	}

	if _, err := wheel.AfterTimeout(id, "third", 50*time.Millisecond); err != nil {
		t.Fatalf("third add failed: %v", err)
	}

	time.Sleep(120 * time.Millisecond)

	if got := fired.Load(); got != 2 {
		t.Fatalf("expected 2 total fires, got %d", got)
	}
}

func TestRescheduler_ContinuousRescheduling(t *testing.T) {
	intervals := []time.Duration{1 * time.Millisecond, 10 * time.Millisecond}
	slots := []int{100, 100}
	wheel := taskwheel.NewHierarchicalTimingWheel[string](intervals, slots)

	var count atomic.Int64
	stop := wheel.StartBatch(1*time.Millisecond, func(ts []*taskwheel.Timer[string]) {
		for range ts {
			n := count.Add(1)
			id := taskwheel.HashID(fmt.Sprintf("iter-%d", n))
			if _, err := wheel.AfterTimeout(id, fmt.Sprintf("iter-%d", n), 20*time.Millisecond); err != nil {
				t.Errorf("reschedule failed at %d: %v", n, err)
			}
		}
	})
	defer stop()

	if _, err := wheel.AfterTimeout(taskwheel.HashID("iter-0"), "iter-0", 20*time.Millisecond); err != nil {
		t.Fatalf("initial schedule failed: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	n := count.Load()
	if n < 5 || n > 9 {
		t.Fatalf("fires out of expected range, got %d (want 5..9)", n)
	}
}

func TestRescheduler_MultipleTasksContinuous(t *testing.T) {
	type periodic struct {
		name      string
		iteration int
	}

	intervals := []time.Duration{1 * time.Millisecond, 10 * time.Millisecond, 100 * time.Millisecond, time.Second}
	slots := []int{100, 100, 100, 60}
	wheel := taskwheel.NewHierarchicalTimingWheel[*periodic](intervals, slots)

	const (
		numTasks       = 100
		delay          = 999 * time.Millisecond
		testDuration   = 5 * time.Second
		expectedMinAvg = 4
	)

	counts := make([]*atomic.Int64, numTasks)
	for i := range counts {
		counts[i] = &atomic.Int64{}
	}

	stop := wheel.StartBatch(1*time.Millisecond, func(ts []*taskwheel.Timer[*periodic]) {
		for _, tim := range ts {
			if tim == nil || tim.Value == nil {
				continue
			}
			p := tim.Value
			counts[idIndex(p.name)].Add(1)
			newID := taskwheel.HashID(fmt.Sprintf("%s-%d", p.name, p.iteration+1))
			_ = newID
			_, err := wheel.AfterTimeout(newID, &periodic{name: p.name, iteration: p.iteration + 1}, delay)
			if err != nil {
				continue
			}
		}
	})
	defer stop()

	for i := 0; i < numTasks; i++ {
		name := fmt.Sprintf("task-%d", i)
		id := taskwheel.HashID(fmt.Sprintf("%s-0", name))
		initial := &periodic{name: name, iteration: 0}
		initialDelay := time.Duration((i%10)+1) * time.Millisecond
		if _, err := wheel.AfterTimeout(id, initial, initialDelay); err != nil {
			t.Fatalf("initial schedule failed for %s: %v", name, err)
		}
	}

	time.Sleep(testDuration)

	var total int64
	var min int64 = 1 << 62
	var max int64
	var zero int
	for i := 0; i < numTasks; i++ {
		v := counts[i].Load()
		total += v
		if v == 0 {
			zero++
		}
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}

	avg := float64(total) / float64(numTasks)

	if zero > 0 {
		t.Fatalf("%d tasks never fired", zero)
	}
	if avg < float64(expectedMinAvg) {
		t.Fatalf("avg fires too low: %.2f (want ≥ %d)", avg, expectedMinAvg)
	}
	if wheel.Len() < numTasks/2 || wheel.Len() > numTasks*2 {
		t.Fatalf("unexpected wheel size: %d", wheel.Len())
	}
}

func idIndex(name string) int {
	var n int
	fmt.Sscanf(name, "task-%d", &n)
	return n
}
