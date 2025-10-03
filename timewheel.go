package taskwheel

import (
	"errors"
	"fmt"
	"hash/fnv"
	"time"
)

type WheelState int

const (
	WheelActive WheelState = iota
	WheelPaused
)

// TimerID is a uniqueID associated with each Timer Instance.
type TimerID uint64

// HashID converts a string to a TimerID using FNV-1a hash.
func HashID(s string) TimerID {
	h := fnv.New64a()
	h.Write([]byte(s))
	return TimerID(h.Sum64())
}

// Timer is scheduled event managed by a Timing Wheel.
// It's support generic payload type [T].
type Timer[T any] struct {
	ID TimerID
	// NextDue is the wall clock when this timer should be fired.
	NextDue int64

	Value T

	prev, next *Timer[T]
	// Index of the slot ()
	slot  uint16
	level uint8
	// 1 byte pad
	_ uint8
}

type slotList[T any] struct {
	head, tail *Timer[T]
}

func (sl *slotList[T]) push(t *Timer[T]) {
	t.next = nil
	t.prev = sl.tail
	if sl.head == nil {
		sl.head = t
	} else {
		sl.tail.next = t
	}
	sl.tail = t
}

func (sl *slotList[T]) remove(t *Timer[T]) {
	if t == nil {
		return
	}
	if t.prev == nil {
		sl.head = t.next
	} else {
		t.prev.next = t.next
	}

	if t.next == nil {
		sl.tail = t.prev
	} else {
		t.next.prev = t.prev
	}

	t.next = nil
	t.prev = nil
}

func (sl *slotList[T]) front() *Timer[T] {
	return sl.head
}

// TimingWheel manages a large number of timers efficiently. It's not thread safe.
type TimingWheel[T any] struct {
	interval    time.Duration
	numSlots    int
	slots       []slotList[T]
	currentSlot int

	timerMap map[TimerID]*Timer[T]
	level    int
	state    WheelState
}

// NewTimingWheel creates a new TimingWheel with the given interval and number of slots.
func NewTimingWheel[T any](interval time.Duration, numSlots, level int) *TimingWheel[T] {
	slots := make([]slotList[T], numSlots)
	return &TimingWheel[T]{
		interval:    interval,
		numSlots:    numSlots,
		slots:       slots,
		currentSlot: 0,
		level:       0,
		state:       WheelActive,
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
	if old, ok := tw.timerMap[id]; ok {
		tw.slots[old.slot].remove(old)
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
	delayInTicks := int((timeout + tw.interval - 1) / tw.interval)
	if delayInTicks < 1 {
		delayInTicks = 1
	}
	slot := (tw.currentSlot + delayInTicks) % tw.numSlots

	timer := &Timer[T]{
		ID:      id,
		Value:   value,
		NextDue: now.Add(timeout).UnixNano(),
		slot:    uint16(slot),
		level:   0,
	}
	tw.slots[slot].push(timer)
	tw.timerMap[id] = timer
	return timer, nil
}

// Remove deletes a timer by its ID.
// Returns true if the timer was found and removed, false if not present.
func (tw *TimingWheel[T]) Remove(id TimerID) bool {
	timer, ok := tw.timerMap[id]
	if !ok {
		return false
	}
	tw.slots[timer.slot].remove(timer)
	delete(tw.timerMap, id)
	return true
}

// Len returns the number of currently scheduled timers.
func (tw *TimingWheel[T]) Len() int {
	return len(tw.timerMap)
}

// Get returns the timer for the given ID if present.
func (tw *TimingWheel[T]) Get(id TimerID) (*Timer[T], bool) {
	timer, ok := tw.timerMap[id]
	return timer, ok
}

// Tick advances the wheel by one slot and returns all timers due at this tick.
// If the wheel is paused, no timers will fire and nil is returned.
func (tw *TimingWheel[T]) Tick() []*Timer[T] {
	if tw.state != WheelActive {
		return nil
	}

	tw.currentSlot = (tw.currentSlot + 1) % tw.numSlots
	slotList := &tw.slots[tw.currentSlot]
	var dueTimers []*Timer[T]

	for t := slotList.front(); t != nil; {
		next := t.next
		dueTimers = append(dueTimers, t)
		slotList.remove(t)
		delete(tw.timerMap, t.ID)
		t = next
	}
	return dueTimers
}

// Reset removes all timers and reset the timing wheel state.
func (tw *TimingWheel[T]) Reset() {
	for slot := range tw.slots {
		for t := tw.slots[slot].front(); t != nil; {
			next := t.next
			tw.slots[slot].remove(t)
			t = next
		}
	}
	tw.timerMap = make(map[TimerID]*Timer[T])
	tw.currentSlot = 0
}

// Pause sets the timer wheel into a paused state.
func (tw *TimingWheel[T]) Pause() {
	tw.state = WheelPaused
}

// Resume sets the timer wheel back to active state.
func (tw *TimingWheel[T]) Resume() {
	tw.state = WheelActive
}

// IsPaused returns true if the timer wheel is currently paused.
func (tw *TimingWheel[T]) IsPaused() bool {
	return tw.state == WheelPaused
}
