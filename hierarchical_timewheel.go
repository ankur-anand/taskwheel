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
	timerMap  map[TimerID]*Timer[T]
	intervals []time.Duration
	maxSpans  []time.Duration
	numLevels int
	state     WheelState
}

// NewHierarchicalTimingWheel returns an initialized Active State Wheel.
// Maximum of 255 levels and 65535 slots per level are supported.
func NewHierarchicalTimingWheel[T any](intervals []time.Duration, slots []int) *HierarchicalTimingWheel[T] {
	if len(intervals) != len(slots) {
		panic("intervals and slots length mismatch")
	}

	if len(intervals) == 0 {
		panic("intervals must not be empty")
	}

	if len(intervals) > 255 {
		panic("intervals must not be more than 255 elements")
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
		if slots[i] > 65535 {
			panic(fmt.Sprintf("slots[%d] must be <= 65535", i))
		}
		levels[i] = newTimingWheel[T](intervals[i], slots[i], i, false)
		span := time.Duration(slots[i]) * intervals[i]
		sum += span
		maxSpans = append(maxSpans, sum)
	}

	return &HierarchicalTimingWheel[T]{
		levels:    levels,
		timerMap:  make(map[TimerID]*Timer[T]),
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
		slot:    uint16(slot),
		level:   uint8(level),
	}

	tw := htw.levels[level]
	tw.slots[slot].push(timer)
	htw.timerMap[id] = timer
	return timer, nil
}

// Remove removes the provided timer from the hierarchical timing wheel and return true if it was present.
func (htw *HierarchicalTimingWheel[T]) Remove(id TimerID) bool {
	htw.mu.Lock()
	defer htw.mu.Unlock()
	return htw.remove(id)
}

func (htw *HierarchicalTimingWheel[T]) remove(id TimerID) bool {
	t, ok := htw.timerMap[id]
	if !ok {
		return false
	}
	htw.levels[t.level].slots[t.slot].remove(t)
	delete(htw.timerMap, id)
	return true
}

// Len returns the total number of timers currently active in hierarchical timing wheel.
func (htw *HierarchicalTimingWheel[T]) Len() int {
	htw.mu.Lock()
	defer htw.mu.Unlock()
	return len(htw.timerMap)
}

// Get returns the timer with the provided ID from the hierarchical timing wheel, if present.
func (htw *HierarchicalTimingWheel[T]) Get(id TimerID) (*Timer[T], bool) {
	htw.mu.Lock()
	defer htw.mu.Unlock()
	t, ok := htw.timerMap[id]
	return t, ok
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
			if i == 0 {
				delete(htw.timerMap, t.ID)
				due = append(due, t)
			} else {
				// down to the right slot of the next lower level.
				nextDue := time.Unix(0, t.NextDue)
				remaining := time.Until(nextDue)
				level, slot, _ := htw.calcPlacement(remaining, time.Now())
				t.level, t.slot = uint8(level), uint16(slot)
				htw.levels[level].slots[slot].push(t)
			}
			t = next
		}
		i++
	}
	return due
}

// calcPlacement determines which level and slot a timer should be placed at.
//
// Example configuration:
//
//	intervals: [1ms, 10ms, 100ms, 1s]
//	slots:     [100, 100, 100, 60]
//
// This creates a hierarchy:
//
//	Level 0: 1ms interval × 100 slots = 100ms span (ticks every 1ms)
//	Level 1: 10ms interval × 100 slots = 10s span (ticks every 100ms when Level 0 wraps)
//	Level 2: 100ms interval × 100 slots = 1000s span (ticks every 10s when Level 1 wraps)
//	Level 3: 1s interval × 60 slots = 60000s span (ticks every 1000s when Level 2 wraps)
//
// Example placements:
//
//	50ms   → Level 0, slot 50  (fires after 50 × 1ms = 50ms)
//	100ms  → Level 1, slot 1   (fires after 1 × 100ms = 100ms)
//	999ms  → Level 1, slot 10  (fires after 10 × 100ms = 1000ms ≈ 999ms)
//	15s    → Level 2, slot 1   (cascades at 10s with 5s remaining, fires at ~15s)
func (htw *HierarchicalTimingWheel[T]) calcPlacement(timeout time.Duration, now time.Time) (level, slot int, due int64) {
	// calculate exact due time first using original timeout
	due = now.Add(timeout).UnixNano()

	// effective tick interval for each level
	// Level 0 ticks at its interval
	// Level N ticks when Level N-1 completes a rotation
	var effectiveInterval time.Duration

	for lvl, tw := range htw.levels {
		if lvl == 0 {
			// Level 0 ticks at the configured interval (e.g., 1ms)
			effectiveInterval = tw.interval
		} else {
			// Higher levels tick when the previous level completes a full rotation.
			// The effective interval is the span of the previous level.
			//
			// CRITICAL: We must use the EFFECTIVE interval of the previous level, not its
			// configured interval. This is because each level's tick rate depends on how
			// long it takes the previous level to complete a full rotation.
			//
			// Example calculation:
			//   Level 0: effectiveInterval = 1ms (configured)
			//   Level 1: effectiveInterval = 100 slots × 1ms = 100ms
			//            (Level 1 ticks when Level 0 completes 100 slots)
			//   Level 2: effectiveInterval = 100 slots × 100ms = 10,000ms = 10s
			//            (Level 2 ticks when Level 1 completes 100 slots)
			//            NOT 100 slots × 10ms = 1s (would use configured interval - WRONG!)
			//
			// If we incorrectly used prevLevel.interval instead of prevEffectiveInterval:
			//   Level 2 would be: 100 slots × 10ms = 1s (WRONG!)
			//   A 15s timer would go to slot 15, firing at 150s instead of 15s!
			//
			// By using prevEffectiveInterval, we get the recursive calculation:
			//   Level 2: 100 slots × 100ms (Level 1's effective) = 10s (CORRECT!)
			//   A 15s timer goes to slot 1, cascades at 10s, fires at 15s!
			prevLevel := htw.levels[lvl-1]
			prevEffectiveInterval := effectiveInterval
			effectiveInterval = time.Duration(prevLevel.numSlots) * prevEffectiveInterval
		}

		// the total time span this level can handle
		// Example: Level 1 spans (100 slots × 100ms) = 10,000ms
		levelSpan := time.Duration(tw.numSlots) * effectiveInterval

		// only this level if timeout fits within its span, or if it's the last level
		if timeout < levelSpan || lvl == htw.numLevels-1 {
			// how many ticks of this level are needed
			var delayInTicks int
			if lvl == 0 {
				// Level 0: Round up to ensure we don't fire too early
				// Example: 999ms / 100ms = 9.99 → rounds up to 10 ticks
				delayInTicks = int((timeout + effectiveInterval - 1) / effectiveInterval)
			} else {
				// Higher levels: Round down, timer will cascade with remaining time
				// Example: 15s / 10s = 1.5 → rounds down to 1 tick, cascades at 10s with 5s remaining
				delayInTicks = int(timeout / effectiveInterval)
			}

			// at least 1 tick (fire at next tick minimum)
			if delayInTicks < 1 {
				delayInTicks = 1
			}

			// Cap at numSlots
			// Example: If timeout > max span, place at furthest slot
			if delayInTicks > tw.numSlots {
				delayInTicks = tw.numSlots
			}

			slot = (tw.currentSlot + delayInTicks) % tw.numSlots
			return lvl, slot, due
		}
	}
	panic("unreachable")
}

// Start begins ticking the hierarchical timing wheel at the specified interval in a new goroutine.
// If wheel is paused Tick() is a no-op and no timers will be fired until resumed.
// The callback is executed synchronously, care should be taken to not block the onTimer Callback.
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
					onTimer(t)
				}
			case <-stopCh:
				return
			}
		}
	}()
	return func() { close(stopCh) }
}

// StartBatch begins ticking the hierarchical timing wheel at the specified interval in a new goroutine.
// If wheel is paused Tick() is a no-op and no timers will be fired until resumed.
// All timers that are due are passed as a batch to a onTimerBatch callback.
// The callback is executed synchronously, care should be taken to not block the onTimerBatch Callback.
func (htw *HierarchicalTimingWheel[T]) StartBatch(tickInterval time.Duration, onTimerBatch func([]*Timer[T])) (stop func()) {
	stopCh := make(chan struct{})
	go func() {
		ticker := time.NewTicker(tickInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				due := htw.Tick()
				if len(due) > 0 {
					onTimerBatch(due)
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
				delete(htw.timerMap, t.ID)
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
		tw.currentSlot = 0
	}
	htw.timerMap = make(map[TimerID]*Timer[T])
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
