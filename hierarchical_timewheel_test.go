package taskwheel

import (
	"fmt"
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

	_, _ = wheel.AfterTimeout("a", "short", 45*time.Millisecond)
	_, _ = wheel.AfterTimeout("b", "long", 2*time.Second)

	time.Sleep(3 * time.Second)
	// Output:
	// Timer short fired
	// Timer long fired
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

	_, err := wheel.AfterTimeout("a", "fire50ms", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("AfterTimeout failed: %v", err)
	}

	_, err = wheel.AfterTimeout("b", "fire1.3s", 1300*time.Millisecond)
	if err != nil {
		t.Fatalf("AfterTimeout failed: %v", err)
	}

	_, err = wheel.AfterTimeout("c", "fire2s", 2*time.Second)
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

	_, err := wheel.AfterTimeout("a", "should_fire", 30*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	_, err = wheel.AfterTimeout("b", "should_NOT_fire", 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	ok := wheel.Remove("b")
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
		fireTimes[string(timer.ID)] = time.Now()
		mu.Unlock()
		wg.Done()
	})
	defer stop()

	_, _ = wheel.AfterTimeout("a", "t1", 80*time.Millisecond)
	_, _ = wheel.AfterTimeout("b", "t2", 160*time.Millisecond)

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

	_, err := wheel.AfterTimeout("a", "should_fire_after_resume", 100*time.Millisecond)
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

	_, _ = wheel.AfterTimeout("a", "A", 50*time.Millisecond)
	_, _ = wheel.AfterTimeout("b", "B", 1*time.Second)
	_, _ = wheel.AfterTimeout("c", "C", 1*time.Second+100*time.Millisecond)

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

	_, _ = wheel.AfterTimeout("a", "A", 50*time.Millisecond)
	_, _ = wheel.AfterTimeout("b", "B", 1*time.Second)
	_, _ = wheel.AfterTimeout("c", "C", 1*time.Second+100*time.Millisecond)

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

	_, ok := wheel.Get("nonexistent")
	if ok {
		t.Fatal("Expected Get to return false for missing timer")
	}

	_, _ = wheel.AfterTimeout("xid", "payload", 200*time.Millisecond)

	tmr, ok := wheel.Get("xid")
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

	_, _ = wheel.AfterTimeout("a", "A", 30*time.Millisecond)
	_, _ = wheel.AfterTimeout("b", "B", 70*time.Millisecond)

	if wheel.Len() != 2 {
		t.Fatalf("Expected 2 timers after insertions")
	}

	wheel.Remove("a")
	if wheel.Len() != 1 {
		t.Fatalf("Expected 1 timer after remove")
	}

	wheel.Reset()
	if wheel.Len() != 0 {
		t.Fatalf("Expected 0 timers after reset")
	}
}
