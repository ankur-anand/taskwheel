package taskwheel

import (
	"container/list"
	"errors"
	"fmt"
	"sync"
	"time"
)

// TimerID is a uniqueID associated with each Timer Instance.
type TimerID string

// Timer is scheduled event managed by a Timing Wheel.
// It's support generic payload type [T].
// The event can be one-shot or periodic.
type Timer[T any] struct {
	ID    TimerID
	Value T
	// NextDue is the wall clock when this timer should be fired.
	NextDue time.Time
	element *list.Element

	// Index of the slot ()
	slot int
}

// TimingWheel is a thread safe timing wheel data structure, that manages a large number of timers
// efficiently.
type TimingWheel[T any] struct {
	interval    time.Duration
	numSlots    int
	slots       []list.List
	currentSlot int
	ticker      *time.Ticker
	mu          sync.Mutex
	stopCh      chan struct{}

	timerMap map[TimerID]*Timer[T]
}

// NewTimingWheel creates a new TimingWheel with the given interval and number of slots.
func NewTimingWheel[T any](interval time.Duration, numSlots int) *TimingWheel[T] {
	slots := make([]list.List, numSlots)
	return &TimingWheel[T]{
		interval:    interval,
		numSlots:    numSlots,
		slots:       slots,
		currentSlot: 0,
		stopCh:      make(chan struct{}),
		timerMap:    make(map[TimerID]*Timer[T]),
	}
}

// AfterTimeout schedules a one-shot timer to fire after a delay.
func (tw *TimingWheel[T]) AfterTimeout(
	id TimerID, value T, timeout time.Duration,
) (*Timer[T], error) {
	return tw.add(id, value, timeout)
}

func (tw *TimingWheel[T]) add(id TimerID, value T, timeout time.Duration) (*Timer[T], error) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	// remove if there is any existing timer with the same ID.
	if old, ok := tw.timerMap[id]; ok {
		if old.element != nil {
			tw.slots[old.slot].Remove(old.element)
		}
		delete(tw.timerMap, id)
	}

	if timeout <= 0 {
		return nil, errors.New("timing wheel timeout must be greater than zero")
	}

	maxTimeout := time.Duration(tw.numSlots) * tw.interval
	if timeout > maxTimeout {
		return nil, fmt.Errorf("timeout %v exceeds wheel max %v", timeout, maxTimeout)
	}

	now := time.Now()
	// rounding up to ensure timer never fires early.
	delayInTicks := int((timeout + tw.interval - 1) / tw.interval)

	if delayInTicks < 0 {
		delayInTicks = 1
	}

	slot := (tw.currentSlot + delayInTicks) % tw.numSlots

	timer := &Timer[T]{
		Value:   value,
		NextDue: now.Add(timeout),
		slot:    slot,
		ID:      id,
	}

	timer.element = tw.slots[slot].PushBack(timer)
	tw.timerMap[id] = timer

	return timer, nil
}

// Tick advances the wheel by one slot and returns all timers due at this tick.
func (tw *TimingWheel[T]) Tick() []*Timer[T] {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	// advance the slot by one
	tw.currentSlot = (tw.currentSlot + 1) % tw.numSlots

	slotList := &tw.slots[tw.currentSlot]
	var dueTimers []*Timer[T]

	for e := slotList.Front(); e != nil; {
		next := e.Next()
		timer := e.Value.(*Timer[T])
		dueTimers = append(dueTimers, timer)
		slotList.Remove(e)
		timer.element = nil
		delete(tw.timerMap, timer.ID)
		e = next
	}

	return dueTimers
}
