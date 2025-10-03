package taskwheel_test

import (
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/taskwheel"
)

func TestTimingWheel_AfterTimeout_And_Tick(t *testing.T) {
	interval := 10 * time.Millisecond
	numSlots := 100

	tw := taskwheel.NewTimingWheel[string](interval, numSlots, 0)
	var mu sync.Mutex
	var fired []string

	id := taskwheel.HashID("42")
	value := "hello world"

	_, err := tw.AfterTimeout(id, value, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("AfterTimeout failed: %v", err)
	}

	go func() {
		for i := 0; i < numSlots*2; i++ {
			time.Sleep(interval)
			for _, timer := range tw.Tick() {
				mu.Lock()
				fired = append(fired, timer.Value)
				mu.Unlock()
			}
		}
	}()

	time.Sleep(120 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(fired) == 0 {
		t.Fatalf("expected timer to fire, but did not")
	}
	if fired[0] != value {
		t.Fatalf("expected %q, got %q", value, fired[0])
	}
}

func TestTimingWheel_AfterTimeout_Errors(t *testing.T) {
	interval := 10 * time.Millisecond
	numSlots := 100
	tw := taskwheel.NewTimingWheel[string](interval, numSlots, 0)

	id := taskwheel.HashID("error")

	if _, err := tw.AfterTimeout(id, "bad", 0); err == nil {
		t.Error("expected error for zero timeout, got nil")
	}

	if _, err := tw.AfterTimeout(id, "bad", -5*time.Millisecond); err == nil {
		t.Error("expected error for negative timeout, got nil")
	}

	maxTimeout := interval * time.Duration(numSlots)
	if _, err := tw.AfterTimeout(id, "bad", maxTimeout+interval); err == nil {
		t.Error("expected error for timeout exceeding max, got nil")
	}

	if _, err := tw.AfterTimeout(id, "good", interval); err != nil {
		t.Errorf("expected no error for valid timeout, got %v", err)
	}
}

func TestTimingWheel_DuplicateID_ReplacesOldTimer(t *testing.T) {
	interval := 10 * time.Millisecond
	numSlots := 100
	tw := taskwheel.NewTimingWheel[string](interval, numSlots, 0)

	id := taskwheel.HashID("dup")

	_, err := tw.AfterTimeout(id, "first", 30*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error for first timer: %v", err)
	}

	_, err = tw.AfterTimeout(id, "second", 40*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error for second timer: %v", err)
	}

	ticks := int((40*time.Millisecond + interval - 1) / interval)
	var fired []string
	for i := 0; i < ticks+2; i++ {
		for _, timer := range tw.Tick() {
			fired = append(fired, timer.Value)
		}
	}

	if len(fired) == 0 {
		t.Fatal("expected timer to fire, got none")
	}
	if len(fired) > 1 {
		t.Fatalf("expected only one timer to fire, got %d: %v", len(fired), fired)
	}
	if fired[0] != "second" {
		t.Errorf("expected 'second' to fire, got %q", fired[0])
	}
}

func TestTimingWheel_Remove_RemovesTimer(t *testing.T) {
	interval := 10 * time.Millisecond
	numSlots := 100
	tw := taskwheel.NewTimingWheel[string](interval, numSlots, 0)
	id := taskwheel.HashID("remove-me")

	_, err := tw.AfterTimeout(id, "bye", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	ok := tw.Remove(id)
	if !ok {
		t.Fatalf("expected Remove to return true")
	}

	for i := 0; i < 10; i++ {
		if timers := tw.Tick(); len(timers) != 0 {
			t.Fatalf("expected no timers to fire, got %+v", timers)
		}
	}

	ok = tw.Remove(id)
	if ok {
		t.Fatalf("expected Remove to return false on second call")
	}
}

func TestTimingWheel_Len(t *testing.T) {
	interval := 10 * time.Millisecond
	numSlots := 100
	tw := taskwheel.NewTimingWheel[string](interval, numSlots, 0)

	if got := tw.Len(); got != 0 {
		t.Fatalf("expected Len() == 0, got %d", got)
	}

	_, err := tw.AfterTimeout(taskwheel.HashID("foo"), "val1", 20*time.Millisecond)
	if err != nil {
		t.Fatalf("AfterTimeout error: %v", err)
	}
	_, err = tw.AfterTimeout(taskwheel.HashID("bar"), "val2", 30*time.Millisecond)
	if err != nil {
		t.Fatalf("AfterTimeout error: %v", err)
	}

	if got := tw.Len(); got != 2 {
		t.Fatalf("expected Len() == 2, got %d", got)
	}

	tw.Remove(taskwheel.HashID("foo"))
	if got := tw.Len(); got != 1 {
		t.Fatalf("expected Len() == 1 after remove, got %d", got)
	}
}

func TestTimingWheel_Get(t *testing.T) {
	interval := 10 * time.Millisecond
	numSlots := 100
	tw := taskwheel.NewTimingWheel[string](interval, numSlots, 0)

	id := taskwheel.HashID("unique-id")
	val := "payload"

	if _, ok := tw.Get(id); ok {
		t.Fatal("expected Get to return false for missing timer")
	}

	_, err := tw.AfterTimeout(id, val, 40*time.Millisecond)
	if err != nil {
		t.Fatalf("AfterTimeout error: %v", err)
	}

	timer, ok := tw.Get(id)
	if !ok {
		t.Fatal("expected Get to find the timer, got false")
	}
	if timer.ID != id || timer.Value != val {
		t.Errorf("Get returned wrong timer: got %+v", timer)
	}

	tw.Remove(id)
	if _, ok := tw.Get(id); ok {
		t.Fatal("expected Get to return false after removal")
	}
}

func TestTimingWheel_Reset(t *testing.T) {
	tw := taskwheel.NewTimingWheel[string](10*time.Millisecond, 10, 0)
	_, _ = tw.AfterTimeout(taskwheel.HashID("a"), "A", 10*time.Millisecond)
	_, _ = tw.AfterTimeout(taskwheel.HashID("b"), "B", 20*time.Millisecond)

	tw.Reset()
	if tw.Len() != 0 {
		t.Fatalf("expected Len()==0 after Reset, got %d", tw.Len())
	}
}

func TestTimingWheel_Tick_PausedState(t *testing.T) {
	interval := 10 * time.Millisecond
	numSlots := 10
	tw := taskwheel.NewTimingWheel[string](interval, numSlots, 0)

	tw.Pause()

	id := taskwheel.HashID("paused-id")
	val := "do-not-fire"

	_, err := tw.AfterTimeout(id, val, interval)
	if err != nil {
		t.Fatalf("AfterTimeout error: %v", err)
	}

	for i := 0; i < numSlots*2; i++ {
		fired := tw.Tick()
		if len(fired) > 0 {
			t.Fatalf("expected no timers to fire while paused, got: %+v", fired)
		}
	}

	tw.Resume()
	fired := tw.Tick()
	found := false
	for _, timer := range fired {
		if timer.ID == id && timer.Value == val {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected timer to fire after resume, but did not")
	}
}

func TestTimingWheel_Tick_ActiveState(t *testing.T) {
	interval := 10 * time.Millisecond
	numSlots := 10
	tw := taskwheel.NewTimingWheel[string](interval, numSlots, 0)

	id := taskwheel.HashID("active-id")
	val := "should-fire"

	_, err := tw.AfterTimeout(id, val, interval)
	if err != nil {
		t.Fatalf("AfterTimeout error: %v", err)
	}

	tw.Resume()

	found := false
	for i := 0; i < numSlots*2; i++ {
		fired := tw.Tick()
		for _, timer := range fired {
			if timer.ID == id && timer.Value == val {
				found = true
			}
		}
		if found {
			break
		}
		time.Sleep(interval)
	}
	if !found {
		t.Fatalf("expected timer to fire in active state, but did not")
	}
}

func TestTimingWheel_IsPaused(t *testing.T) {
	tw := taskwheel.NewTimingWheel[string](10*time.Millisecond, 10, 0)

	if tw.IsPaused() {
		t.Fatal("expected IsPaused() to be false by default")
	}

	tw.Pause()
	if !tw.IsPaused() {
		t.Fatal("expected IsPaused() to be true after Pause()")
	}

	tw.Resume()
	if tw.IsPaused() {
		t.Fatal("expected IsPaused() to be false after Resume()")
	}
}
