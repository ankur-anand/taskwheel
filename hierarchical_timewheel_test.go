package taskwheel

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

func ExampleHierarchicalTimingWheel() {
	intervals := []time.Duration{10 * time.Millisecond, 1 * time.Second}
	slots := []int{100, 60}
	wheel := NewHierarchicalTimingWheel[string](intervals, slots)

	stop := wheel.Start(10*time.Millisecond, func(timer *Timer[string]) {
		fmt.Printf("Timer %s fired\n", timer.Value)
	})
	defer stop()

	_, _ = wheel.AfterTimeout(HashID("a"), "short", 45*time.Millisecond)
	_, _ = wheel.AfterTimeout(HashID("b"), "long", 2*time.Second)

	time.Sleep(3 * time.Second)
	// Output:
	// Timer short fired
	// Timer long fired
}

func ExampleHierarchicalTimingWheel_StartBatch() {
	intervals := []time.Duration{10 * time.Millisecond, 1 * time.Second}
	slots := []int{100, 60}
	wheel := NewHierarchicalTimingWheel[string](intervals, slots)

	stop := wheel.StartBatch(10*time.Millisecond, func(timers []*Timer[string]) {
		fmt.Printf("Batch of %d timers fired\n", len(timers))

		for _, timer := range timers {
			fmt.Printf("- Timer %s fired\n", timer.Value)
		}

		// or use can use pool or any other pattern
		//for _, timer := range timers {
		//	workerpool <- timer
		//}
	})

	defer stop()

	_, _ = wheel.AfterTimeout(HashID("a"), "short", 45*time.Millisecond)
	_, _ = wheel.AfterTimeout(HashID("b"), "long", 50*time.Millisecond)
	time.Sleep(3 * time.Second)
	// Output:
	// Batch of 2 timers fired
	// - Timer short fired
	// - Timer long fired
}

func TestHierarchicalTimingWheel_FiresTimersCorrectly(t *testing.T) {
	intervals := []time.Duration{10 * time.Millisecond, 1 * time.Second, 1 * time.Minute}
	slots := []int{100, 60, 60}
	wheel := NewHierarchicalTimingWheel[string](intervals, slots)

	var mu sync.Mutex
	var fired []string

	done := make(chan struct{})

	stop := wheel.Start(10*time.Millisecond, func(timer *Timer[string]) {
		mu.Lock()
		fired = append(fired, timer.Value)
		if len(fired) == 3 {
			close(done)
		}
		mu.Unlock()
	})
	defer stop()

	_, err := wheel.AfterTimeout(HashID("a"), "fire50ms", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("AfterTimeout failed: %v", err)
	}

	_, err = wheel.AfterTimeout(HashID("b"), "fire1.3s", 1300*time.Millisecond)
	if err != nil {
		t.Fatalf("AfterTimeout failed: %v", err)
	}

	_, err = wheel.AfterTimeout(HashID("c"), "fire2s", 2*time.Second)
	if err != nil {
		t.Fatalf("AfterTimeout failed: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("Timeout waiting for timers to fire: got %v", fired)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(fired) != 3 {
		t.Fatalf("Expected 3 timers to fire, got %d: %v", len(fired), fired)
	}

	want := []string{"fire50ms", "fire1.3s", "fire2s"}
	if len(fired) != len(want) {
		t.Fatalf("Expected %d timers to fire, got %d: %v", len(want), len(fired), fired)
	}

	found := map[string]bool{}
	for _, v := range fired {
		found[v] = true
	}
	for _, v := range want {
		if !found[v] {
			t.Errorf("Missing fired timer: %v", v)
		}
	}
}

func TestHierarchicalTimingWheel_Remove(t *testing.T) {
	intervals := []time.Duration{10 * time.Millisecond, 1 * time.Second}
	slots := []int{100, 60}
	wheel := NewHierarchicalTimingWheel[string](intervals, slots)

	var mu sync.Mutex
	var fired []string

	stop := wheel.Start(10*time.Millisecond, func(timer *Timer[string]) {
		mu.Lock()
		fired = append(fired, timer.Value)
		mu.Unlock()
	})
	defer stop()

	_, err := wheel.AfterTimeout(HashID("a"), "should_fire", 30*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	_, err = wheel.AfterTimeout(HashID("b"), "should_NOT_fire", 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	ok := wheel.Remove(HashID("b"))
	if !ok {
		t.Fatal("Remove failed for timer b")
	}

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(fired) != 1 || fired[0] != "should_fire" {
		t.Fatalf("Remove test failed, fired: %v", fired)
	}
}

func TestHierarchicalTimingWheel_OrderAndPrecision(t *testing.T) {
	intervals := []time.Duration{10 * time.Millisecond, 1 * time.Second}
	slots := []int{100, 60}
	wheel := NewHierarchicalTimingWheel[string](intervals, slots)

	var wg sync.WaitGroup
	wg.Add(2)
	var mu sync.Mutex
	var fireTimes = make(map[string]time.Time)

	start := time.Now()
	stop := wheel.Start(10*time.Millisecond, func(timer *Timer[string]) {
		mu.Lock()
		fireTimes[timer.Value] = time.Now()
		mu.Unlock()
		wg.Done()
	})
	defer stop()

	_, _ = wheel.AfterTimeout(HashID("a"), "a", 80*time.Millisecond)
	_, _ = wheel.AfterTimeout(HashID("b"), "b", 160*time.Millisecond)

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	elapsedA := fireTimes["a"].Sub(start)
	elapsedB := fireTimes["b"].Sub(start)

	if elapsedA < 80*time.Millisecond-10*time.Millisecond {
		t.Errorf("Timer a fired too early: %v", elapsedA)
	}
	if elapsedB < 160*time.Millisecond-10*time.Millisecond {
		t.Errorf("Timer b fired too early: %v", elapsedB)
	}
}

func TestHierarchicalTimingWheel_Pause_And_Resume(t *testing.T) {
	intervals := []time.Duration{10 * time.Millisecond, 1 * time.Second}
	slots := []int{100, 60}
	wheel := NewHierarchicalTimingWheel[string](intervals, slots)

	var fired []string
	var mu sync.Mutex
	stop := wheel.Start(10*time.Millisecond, func(timer *Timer[string]) {
		mu.Lock()
		fired = append(fired, timer.Value)
		mu.Unlock()
	})
	defer stop()

	_, err := wheel.AfterTimeout(HashID("a"), "should_fire_after_resume", 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	wheel.Pause()

	mu.Lock()
	if len(fired) != 0 {
		t.Errorf("Expected no timers to fire while paused, but got: %v", fired)
	}
	mu.Unlock()

	wheel.Resume()

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(fired) == 0 || fired[0] != "should_fire_after_resume" {
		t.Fatalf("Expected timer to fire after resume, got: %v", fired)
	}
}

func TestHierarchicalTimingWheel_IsPaused(t *testing.T) {
	intervals := []time.Duration{10 * time.Millisecond, 1 * time.Second}
	slots := []int{100, 60}
	wheel := NewHierarchicalTimingWheel[string](intervals, slots)

	if wheel.IsPaused() {
		t.Fatalf("wheel should not be paused initially")
	}

	wheel.Pause()
	if !wheel.IsPaused() {
		t.Fatalf("wheel should be paused after Pause()")
	}

	wheel.Resume()
	if wheel.IsPaused() {
		t.Fatalf("wheel should not be paused after Resume()")
	}
}

func TestHierarchicalTimingWheel_Drain(t *testing.T) {
	intervals := []time.Duration{10 * time.Millisecond, 1 * time.Second}
	slots := []int{100, 60}
	wheel := NewHierarchicalTimingWheel[string](intervals, slots)

	_, _ = wheel.AfterTimeout(HashID("a"), "A", 50*time.Millisecond)
	_, _ = wheel.AfterTimeout(HashID("b"), "B", 1*time.Second)
	_, _ = wheel.AfterTimeout(HashID("c"), "C", 1*time.Second+100*time.Millisecond)

	drained := wheel.Drain()
	found := map[string]bool{"A": false, "B": false, "C": false}
	for _, tmr := range drained {
		found[tmr.Value] = true
	}

	for k, v := range found {
		if !v {
			t.Fatalf("Drain did not return timer %s", k)
		}
	}

	stop := wheel.Start(10*time.Millisecond, func(timer *Timer[string]) {
		t.Errorf("No timer should fire after drain, but got: %v", timer)
	})
	defer stop()
	time.Sleep(70 * time.Millisecond)
}

func TestHierarchicalTimingWheel_Reset(t *testing.T) {
	intervals := []time.Duration{10 * time.Millisecond, 1 * time.Second}
	slots := []int{100, 60}
	wheel := NewHierarchicalTimingWheel[string](intervals, slots)

	_, _ = wheel.AfterTimeout(HashID("a"), "A", 50*time.Millisecond)
	_, _ = wheel.AfterTimeout(HashID("b"), "B", 1*time.Second)
	_, _ = wheel.AfterTimeout(HashID("c"), "C", 1*time.Second+100*time.Millisecond)

	wheel.Reset()
	if got := wheel.Len(); got != 0 {
		t.Fatalf("Expected 0 timers after Reset, got %d", got)
	}

	stop := wheel.Start(10*time.Millisecond, func(timer *Timer[string]) {
		t.Errorf("No timer should fire after reset, but got: %v", timer)
	})
	defer stop()
	time.Sleep(70 * time.Millisecond)
}

func TestHierarchicalTimingWheel_Get(t *testing.T) {
	intervals := []time.Duration{10 * time.Millisecond, 1 * time.Second}
	slots := []int{100, 60}
	wheel := NewHierarchicalTimingWheel[string](intervals, slots)

	_, ok := wheel.Get(HashID("nonexistent"))
	if ok {
		t.Fatal("Expected Get to return false for missing timer")
	}

	_, _ = wheel.AfterTimeout(HashID("xid"), "payload", 200*time.Millisecond)

	tmr, ok := wheel.Get(HashID("xid"))
	if !ok || tmr.Value != "payload" {
		t.Fatal("Get did not find the expected timer")
	}
}

func TestHierarchicalTimingWheel_Len(t *testing.T) {
	intervals := []time.Duration{10 * time.Millisecond, 1 * time.Second}
	slots := []int{100, 60}
	wheel := NewHierarchicalTimingWheel[string](intervals, slots)

	if wheel.Len() != 0 {
		t.Fatalf("Expected 0 timers at start")
	}

	_, _ = wheel.AfterTimeout(HashID("a"), "A", 30*time.Millisecond)
	_, _ = wheel.AfterTimeout(HashID("b"), "B", 70*time.Millisecond)

	if wheel.Len() != 2 {
		t.Fatalf("Expected 2 timers after insertions")
	}

	wheel.Remove(HashID("a"))
	if wheel.Len() != 1 {
		t.Fatalf("Expected 1 timer after remove")
	}

	wheel.Reset()
	if wheel.Len() != 0 {
		t.Fatalf("Expected 0 timers after reset")
	}
}

func TestHierarchicalTimingWheel_StartBatch(t *testing.T) {
	intervals := []time.Duration{10 * time.Millisecond, 1 * time.Second}
	slots := []int{100, 60}
	wheel := NewHierarchicalTimingWheel[string](intervals, slots)

	var mu sync.Mutex
	var batches []int
	var allFired []string

	stop := wheel.StartBatch(10*time.Millisecond, func(timers []*Timer[string]) {
		mu.Lock()
		batches = append(batches, len(timers))
		for _, t := range timers {
			allFired = append(allFired, t.Value)
		}
		mu.Unlock()
	})

	defer stop()
	_, _ = wheel.AfterTimeout(HashID("a"), "A", 48*time.Millisecond)
	_, _ = wheel.AfterTimeout(HashID("b"), "B", 45*time.Millisecond)
	_, _ = wheel.AfterTimeout(HashID("c"), "C", 100*time.Millisecond)

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(batches) != 2 {
		t.Fatalf("Expected 2 timer batchess, got %d", len(batches))
	}

	if len(allFired) != 3 {
		t.Fatalf("Expected 3 timer, got %d", len(allFired))
	}

	if !reflect.DeepEqual(allFired, []string{"A", "B", "C"}) {
		t.Fatalf("Expected all fired timer, got %v", allFired)
	}
}

func TestHierarchicalTimingWheel_15SecondTimer(t *testing.T) {
	intervals := []time.Duration{1 * time.Millisecond, 10 * time.Millisecond, 100 * time.Millisecond, time.Second}
	slots := []int{100, 100, 100, 60}
	wheel := NewHierarchicalTimingWheel[string](intervals, slots)
	timeout := 15 * time.Second
	now := time.Now()
	level, slot, dueNano := wheel.calcPlacement(timeout, now)
	if level != 2 {
		t.Errorf("expected level 2, got %d", level)
	}
	if slot != 1 {
		t.Errorf("expected slot 1, got %d", slot)
	}
	expectedDue := now.Add(timeout)
	actualDue := time.Unix(0, dueNano)
	if diff := actualDue.Sub(expectedDue); diff < -10*time.Millisecond || diff > 10*time.Millisecond {
		t.Errorf("due mismatch: expected %v, got %v", expectedDue, actualDue)
	}
	var firedAt time.Time
	var mu sync.Mutex
	done := make(chan struct{})
	scheduledAt := time.Now()
	stop := wheel.Start(1*time.Millisecond, func(timer *Timer[string]) {
		mu.Lock()
		firedAt = time.Now()
		mu.Unlock()
		close(done)
	})
	defer stop()
	id := HashID("15s-test")
	_, err := wheel.AfterTimeout(id, "15s-timer", timeout)
	if err != nil {
		t.Fatalf("AfterTimeout failed: %v", err)
	}
	select {
	case <-done:
		mu.Lock()
		delay := firedAt.Sub(scheduledAt)
		mu.Unlock()
		if delay < 14900*time.Millisecond || delay > 15100*time.Millisecond {
			t.Errorf("timer fired wrong time: %v", delay)
		}
	case <-time.After(16 * time.Second):
		t.Errorf("timer did not fire")
	}
}

func TestHierarchicalTimingWheel_PlacementEdgeCases(t *testing.T) {
	intervals := []time.Duration{1 * time.Millisecond, 10 * time.Millisecond, 100 * time.Millisecond, time.Second}
	slots := []int{100, 100, 100, 60}
	wheel := NewHierarchicalTimingWheel[string](intervals, slots)
	tests := []struct {
		name          string
		timeout       time.Duration
		expectedLevel int
		expectedSlot  int
	}{
		{"1ms", 1 * time.Millisecond, 0, 1},
		{"99ms", 99 * time.Millisecond, 0, 99},
		{"100ms", 100 * time.Millisecond, 1, 1},
		{"500ms", 500 * time.Millisecond, 1, 5},
		{"9999ms", 9999 * time.Millisecond, 1, 99},
		{"10s", 10 * time.Second, 2, 1},
		{"25s", 25 * time.Second, 2, 2},
		{"95s", 95 * time.Second, 2, 9},
		{"1000s", 1000 * time.Second, 3, 1},
		{"30000s", 30000 * time.Second, 3, 30},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now()
			level, slot, dueNano := wheel.calcPlacement(tt.timeout, now)
			if level != tt.expectedLevel {
				t.Errorf("level got %d, want %d", level, tt.expectedLevel)
			}
			if slot != tt.expectedSlot {
				t.Errorf("slot got %d, want %d", slot, tt.expectedSlot)
			}
			expectedDue := now.Add(tt.timeout)
			actualDue := time.Unix(0, dueNano)
			if diff := actualDue.Sub(expectedDue); diff < -10*time.Millisecond || diff > 10*time.Millisecond {
				t.Errorf("due mismatch: expected %v, got %v", expectedDue, actualDue)
			}
		})
	}
}

func TestHierarchicalTimingWheel_CascadingAccuracy(t *testing.T) {
	intervals := []time.Duration{10 * time.Millisecond, 100 * time.Millisecond, time.Second}
	slots := []int{10, 10, 10}
	tests := []struct {
		name     string
		timeout  time.Duration
		minDelay time.Duration
		maxDelay time.Duration
	}{
		{"250ms", 250 * time.Millisecond, 240 * time.Millisecond, 260 * time.Millisecond},
		{"1.5s", 1500 * time.Millisecond, 1480 * time.Millisecond, 1520 * time.Millisecond},
		{"3.7s", 3700 * time.Millisecond, 3680 * time.Millisecond, 3720 * time.Millisecond},
		{"5.25s", 5250 * time.Millisecond, 5230 * time.Millisecond, 5270 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wheel := NewHierarchicalTimingWheel[string](intervals, slots)
			var firedAt time.Time
			var mu sync.Mutex
			done := make(chan struct{})
			scheduledAt := time.Now()
			stop := wheel.Start(10*time.Millisecond, func(timer *Timer[string]) {
				mu.Lock()
				firedAt = time.Now()
				mu.Unlock()
				close(done)
			})
			defer stop()
			id := HashID(tt.name)
			_, err := wheel.AfterTimeout(id, tt.name, tt.timeout)
			if err != nil {
				t.Fatalf("AfterTimeout failed: %v", err)
			}
			select {
			case <-done:
				mu.Lock()
				delay := firedAt.Sub(scheduledAt)
				mu.Unlock()
				if delay < tt.minDelay || delay > tt.maxDelay {
					t.Errorf("delay out of range: %v", delay)
				}
			case <-time.After(tt.timeout + 500*time.Millisecond):
				t.Errorf("timer did not fire")
			}
		})
	}
}

func TestHierarchicalTimingWheel_RoundingBehavior(t *testing.T) {
	intervals := []time.Duration{10 * time.Millisecond, 100 * time.Millisecond}
	slots := []int{10, 10}
	wheel := NewHierarchicalTimingWheel[string](intervals, slots)
	tests := []struct {
		name          string
		timeout       time.Duration
		expectedLevel int
		expectedSlot  int
	}{
		{"55ms", 55 * time.Millisecond, 0, 6},
		{"550ms", 550 * time.Millisecond, 1, 5},
		{"51ms", 51 * time.Millisecond, 0, 6},
		{"510ms", 510 * time.Millisecond, 1, 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now()
			level, slot, _ := wheel.calcPlacement(tt.timeout, now)
			if level != tt.expectedLevel {
				t.Errorf("level got %d, want %d", level, tt.expectedLevel)
			}
			if slot != tt.expectedSlot {
				t.Errorf("slot got %d, want %d", slot, tt.expectedSlot)
			}
		})
	}
}
