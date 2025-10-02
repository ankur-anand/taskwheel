## taskwheel

[![Go Reference](https://pkg.go.dev/badge/github.com/ankur-anand/taskwheel.svg)](https://pkg.go.dev/github.com/ankur-anand/taskwheel)
[![Go Report Card](https://goreportcard.com/badge/github.com/ankur-anand/taskwheel)](https://goreportcard.com/report/github.com/ankur-anand/taskwheel)

A high-performance, generic **Hierarchical Timing Wheel** implementation in Go for efficient timer management at scale.

### Installation

```bash
go get github.com/ankur-anand/taskwheel
```

### Quick Start

```go
package main

import (
	"fmt"
	"time"

	"github.com/ankur-anand/taskwheel"
)

func main() {
	// Create hierarchical timing wheel
	intervals := []time.Duration{10 * time.Millisecond, 1 * time.Second}
	slots := []int{100, 60}
	wheel := taskwheel.NewHierarchicalTimingWheel[string](intervals, slots)

	// Start the wheel
	stop := wheel.Start(10*time.Millisecond, func(timer *taskwheel.Timer[string]) {
		fmt.Printf("Timer fired: %s\n", timer.Value)
	})
	defer stop()

	// Schedule timers
	wheel.AfterTimeout("task1", "Process payment", 100*time.Millisecond)
	wheel.AfterTimeout("task2", "Send email", 500*time.Millisecond)

	time.Sleep(1 * time.Second)
}
```

### High-Throughput Usage (10,000+ timers/sec)

For production systems with high timer volumes, use `StartBatch()` with a worker pool:

```go
package main

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/ankur-anand/taskwheel"
)

func main() {
	intervals := []time.Duration{10 * time.Millisecond, 100 * time.Millisecond, time.Second}
	slots := []int{10, 100, 60}
	wheel := taskwheel.NewHierarchicalTimingWheel[string](intervals, slots)

	// Create worker pool
	workerPool := make(chan *taskwheel.Timer[string], 1000)
	var wg sync.WaitGroup

	numWorkers := runtime.NumCPU() * 2
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for timer := range workerPool {
				// process timer
				processTask(timer)
			}
		}()
	}

	// start with batch callback
	stop := wheel.StartBatch(10*time.Millisecond, func(timers []*taskwheel.Timer[string]) {
		for _, t := range timers {
			workerPool <- t
		}
	})

	// schedule timers
	for i := 0; i < 10000; i++ {
		wheel.AfterTimeout(
			taskwheel.TimerID(fmt.Sprintf("task-%d", i)),
			fmt.Sprintf("Task %d", i),
			time.Duration(i)*time.Millisecond,
		)
	}

	time.Sleep(15 * time.Second)
	stop()
	close(workerPool)
	wg.Wait()
}

func processTask(timer *taskwheel.Timer[string]) {
	// business logic here
}
```

### Performance Comparison

```bash
goos: darwin
goarch: arm64
pkg: github.com/ankur-anand/taskwheel
cpu: Apple M2 Pro
BenchmarkNativeTimers/1K_timers_100ms-10         	      10	 102039642 ns/op	  191393 B/op	    2091 allocs/op
BenchmarkNativeTimers/10K_timers_100ms-10        	       9	 114114778 ns/op	 1820984 B/op	   20260 allocs/op
BenchmarkNativeTimers/100K_timers_1s-10          	       1	1090769709 ns/op	39694704 B/op	  240698 allocs/op
BenchmarkTimingWheelAfterTimeout/1K_timers_100ms-10         	      10	 110101579 ns/op	  315608 B/op	    1056 allocs/op
BenchmarkTimingWheelAfterTimeout/10K_timers_100ms-10        	       9	 111119176 ns/op	 2857496 B/op	   10181 allocs/op
BenchmarkTimingWheelAfterTimeout/100K_timers_100ms-10       	       9	 122693727 ns/op	26476164 B/op	  101094 allocs/op
BenchmarkMemoryComparison/Native_10K_timers-10              	      10	 111232592 ns/op	 1429328 B/op	   20003 allocs/op
BenchmarkMemoryComparison/TimingWheel_10K_timers-10         	      10	 110203346 ns/op	 2857505 B/op	   10181 allocs/op
```

| Workload              | Metric     | NativeTimers | TimingWheel | Difference |
|-----------------------|------------|--------------|-------------|------------|
| **1K timers (100ms)** | Time/op    | 102 ms       | 110 ms      | +8% slower |
|                       | Mem/op     | 191 KB       | 316 KB      | +65% more  |
|                       | Allocs/op  | 2.1 K        | 1.1 K       | -50% fewer |
| **10K timers (100ms)**| Time/op    | 114 ms       | 111 ms      | -3% faster |
|                       | Mem/op     | 1.8 MB       | 2.9 MB      | +57% more  |
|                       | Allocs/op  | 20.3 K       | 10.2 K      | -50% fewer |
| **100K timers (1s)**  | Time/op    | 1.09 s       | 0.12 s      | -89% faster |
|                       | Mem/op     | 39.7 MB      | 26.5 MB     | -33% less  |
|                       | Allocs/op  | 240.7 K      | 101.1 K     | -58% fewer |

## Advanced Usage

### Custom Payload Types

```go
type Task struct {
    UserID   string
    Action   string
    Priority int
}

wheel := taskwheel.NewHierarchicalTimingWheel[Task](intervals, slots)
wheel.AfterTimeout("task1", Task{
    UserID:   "user123",
    Action:   "send_email",
    Priority: 1,
}, 5*time.Second)
```

### Priority-Based Processing

```go
stop := wheel.StartBatch(10*time.Millisecond, func(timers []*taskwheel.Timer[Task]) {
    // Sort by priority
    sort.Slice(timers, func(i, j int) bool {
        return timers[i].Value.Priority > timers[j].Value.Priority
    })
    
    for _, t := range timers {
        processTask(t)
    }
})
```

## License

MIT License - see [LICENSE](LICENSE) file for details

## Credits

Inspired by:
- [Kafka's Hierarchical Timing Wheels](https://www.confluent.io/blog/apache-kafka-purgatory-hierarchical-timing-wheels/)
- ["Hashed and Hierarchical Timing Wheels" paper](http://www.cs.columbia.edu/~nahum/w6998/papers/ton97-timing-wheels.pdf)

