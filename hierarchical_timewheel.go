package taskwheel

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// HierarchicalTimingWheel manages a large number of timers across multiple levels of wheels.
type HierarchicalTimingWheel[T any] struct {
	mu        sync.Mutex
	levels    []*TimingWheel[T]
	intervals []time.Duration
	maxSpans  []time.Duration
	numLevels int
	state     WheelState
}

// NewHierarchicalTimingWheel returns an initialized Active State Wheel.
func NewHierarchicalTimingWheel[T any](intervals []time.Duration, slots []int) *HierarchicalTimingWheel[T] {
	if len(intervals) != len(slots) {
		panic("intervals and slots length mismatch")
	}

	var maxSpans []time.Duration
	sum := time.Duration(0)
	levels := make([]*TimingWheel[T], len(intervals))

	for i := range intervals {
		if intervals[i] <= 0 {
			panic(fmt.Sprintf("interval[%d] must be > 0", i))
		}
		if slots[i] <= 0 {
			panic(fmt.Sprintf("slots[%d] must be > 0", i))
		}
		levels[i] = NewTimingWheel[T](intervals[i], slots[i], i)
		span := time.Duration(slots[i]) * intervals[i]
		sum += span
		maxSpans = append(maxSpans, sum)
	}

	return &HierarchicalTimingWheel[T]{
		levels:    levels,
		intervals: intervals,
		maxSpans:  maxSpans,
		numLevels: len(levels),
		state:     WheelActive,
	}
}

// AfterTimeout schedules a one-shot timer to fire after a delay.
func (htw *HierarchicalTimingWheel[T]) AfterTimeout(id TimerID, value T, timeout time.Duration) (*Timer[T], error) {
	htw.mu.Lock()
	defer htw.mu.Unlock()
	now := time.Now()
	htw.remove(id)

	if timeout <= 0 {
		return nil, errors.New("timeout must be > 0")
	}

	level, slot, due := htw.calcPlacement(timeout, now)
	timer := &Timer[T]{
		ID:      id,
		Value:   value,
		NextDue: due,
		slot:    slot,
		level:   level,
	}

	tw := htw.levels[level]
	tw.slots[slot].push(timer)
	tw.timerMap[id] = timer
	return timer, nil
}

// Remove removes the provided timer from the hierarchical timing wheel and return true if it was present.
func (htw *HierarchicalTimingWheel[T]) Remove(id TimerID) bool {
	htw.mu.Lock()
	defer htw.mu.Unlock()
	return htw.remove(id)
}

func (htw *HierarchicalTimingWheel[T]) remove(id TimerID) bool {
	for _, tw := range htw.levels {
		if t, ok := tw.timerMap[id]; ok {
			tw.slots[t.slot].remove(t)
			delete(tw.timerMap, id)
			return true
		}
	}
	return false
}

// Len returns the total number of timers currently active in hierarchical timing wheel.
func (htw *HierarchicalTimingWheel[T]) Len() int {
	htw.mu.Lock()
	defer htw.mu.Unlock()
	sum := 0
	for _, tw := range htw.levels {
		sum += len(tw.timerMap)
	}
	return sum
}

// Get returns the timer with the provided ID from the hierarchical timing wheel, if present.
func (htw *HierarchicalTimingWheel[T]) Get(id TimerID) (*Timer[T], bool) {
	htw.mu.Lock()
	defer htw.mu.Unlock()
	for _, tw := range htw.levels {
		if t, ok := tw.timerMap[id]; ok {
			return t, true
		}
	}
	return nil, false
}

// Tick advances the hierarchical timing wheel by one slot at each level.
// It returns of timers that are due and should be fired at this tick.
func (htw *HierarchicalTimingWheel[T]) Tick() []*Timer[T] {
	htw.mu.Lock()
	defer htw.mu.Unlock()

	if htw.state != WheelActive {
		return nil
	}

	var due []*Timer[T]
	carry := true
	i := 0
	for carry && i < htw.numLevels {
		tw := htw.levels[i]
		tw.currentSlot = (tw.currentSlot + 1) % tw.numSlots
		carry = tw.currentSlot == 0
		slotList := &tw.slots[tw.currentSlot]
		for t := slotList.front(); t != nil; {
			next := t.next
			slotList.remove(t)
			delete(tw.timerMap, t.ID)
			if i == 0 {
				due = append(due, t)
			} else {
				// down to the right slot of the next lower level.
				remaining := time.Until(t.NextDue)
				level, slot, _ := htw.calcPlacement(remaining, time.Now())
				t.level, t.slot = level, slot
				htw.levels[level].slots[slot].push(t)
				htw.levels[level].timerMap[t.ID] = t
			}
			t = next
		}
		i++
	}
	return due
}

func (htw *HierarchicalTimingWheel[T]) calcPlacement(timeout time.Duration, now time.Time) (level, slot int, due time.Time) {
	for lvl, tw := range htw.levels {
		levelSpan := time.Duration(tw.numSlots) * tw.interval
		if timeout < levelSpan || lvl == htw.numLevels-1 {
			delayInTicks := int((timeout + tw.interval - 1) / tw.interval)
			if delayInTicks < 1 {
				delayInTicks = 1
			}
			slot = (tw.currentSlot + delayInTicks) % tw.numSlots
			due = now.Add(timeout)
			return lvl, slot, due
		}
		timeout -= levelSpan
	}
	panic("unreachable")
}

// Start begins ticking the hierarchical timing wheel at the specified interval in a new goroutine.
// If wheel is paused Tick() is a no-op and no timers will be fired until resumed.
func (htw *HierarchicalTimingWheel[T]) Start(tickInterval time.Duration, onTimer func(*Timer[T])) (stop func()) {
	stopCh := make(chan struct{})
	go func() {
		ticker := time.NewTicker(tickInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				due := htw.Tick()
				for _, t := range due {
					go onTimer(t)
				}
			case <-stopCh:
				return
			}
		}
	}()
	return func() { close(stopCh) }
}

// Drain removes and returns all scheduled timers from all levels of hierarchical timing wheel.
func (htw *HierarchicalTimingWheel[T]) Drain() []*Timer[T] {
	htw.mu.Lock()
	defer htw.mu.Unlock()
	var timers []*Timer[T]
	for _, tw := range htw.levels {
		for i := range tw.slots {
			for t := tw.slots[i].front(); t != nil; {
				next := t.next
				tw.slots[i].remove(t)
				delete(tw.timerMap, t.ID)
				timers = append(timers, t)
				t = next
			}
		}
	}
	return timers
}

// Reset removes all scheduled timers from hierarchical timing wheel.
func (htw *HierarchicalTimingWheel[T]) Reset() {
	htw.mu.Lock()
	defer htw.mu.Unlock()
	for _, tw := range htw.levels {
		for i := range tw.slots {
			for t := tw.slots[i].front(); t != nil; {
				next := t.next
				tw.slots[i].remove(t)
				t = next
			}
		}
		tw.timerMap = make(map[TimerID]*Timer[T])
		tw.currentSlot = 0
	}
}

// Pause move the hierarchical timing wheel into the Paused state.
func (htw *HierarchicalTimingWheel[T]) Pause() {
	htw.setState(WheelPaused)
}

// Resume move the hierarchical timing wheel back to the Active state.
func (htw *HierarchicalTimingWheel[T]) Resume() {
	htw.setState(WheelActive)
}

// IsPaused returns true if hierarchical timing wheel is currently in the Paused state.
func (htw *HierarchicalTimingWheel[T]) IsPaused() bool {
	htw.mu.Lock()
	defer htw.mu.Unlock()
	return htw.state == WheelPaused
}

func (htw *HierarchicalTimingWheel[T]) setState(newState WheelState) {
	htw.mu.Lock()
	defer htw.mu.Unlock()
	htw.state = newState
}
