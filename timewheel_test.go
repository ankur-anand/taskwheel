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

	tw := taskwheel.NewTimingWheel[string](interval, numSlots)
	var mu sync.Mutex
	var fired []string

	id := taskwheel.TimerID("42")
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
	tw := taskwheel.NewTimingWheel[string](interval, numSlots)

	id := taskwheel.TimerID("error")

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
	tw := taskwheel.NewTimingWheel[string](interval, numSlots)

	id := taskwheel.TimerID("dup")

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
