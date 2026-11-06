# Concurrent Priority Task Scheduler (C++17)

A thread-safe priority task scheduler that accepts tasks from producer code, queues them with priority ordering and aging (anti-starvation), and executes them concurrently across a configurable pool of worker threads. Fully verified **TSan-clean** and **ASan/UBSan-clean**.

## Features

- **Fixed-size worker pool** — configurable worker count (1, 2, 4, 8, or more).
- **Thread-safe bounded queue** — built with `std::mutex` and `std::condition_variable`; workers block rather than busy-wait when no tasks are available.
- **Multi-policy backpressure**:
  - **Block:** `submit()` / `submit_with_priority()` blocks when full, preventing unbounded memory growth.
  - **Reject (Non-blocking):** `try_submit()` returns `std::nullopt` immediately if the queue is full.
  - **Timeout:** `submit_for(timeout)` waits up to a specified duration before giving up.
- **Priority scheduling with aging (anti-starvation)**:
  - Higher-priority tasks execute first; equal-priority tasks retain submission (FIFO) order.
  - **Dynamic Aging:** Calculates `effective_priority = priority + (now - enqueued) / aging_interval`. As tasks wait in the queue, their priority dynamically elevates, guaranteeing that low-priority work cannot be starved by high-priority bursts.
- **Dual shutdown modes**:
  - `shutdown(ShutdownMode::Drain)` — stops accepting new work, lets all queued tasks finish, and joins workers cleanly (safer for correctness).
  - `shutdown(ShutdownMode::Cancel)` — stops accepting work, discards unstarted queued tasks (setting their futures to `broken_promise`), and joins workers as soon as active tasks finish (faster for rapid teardown).
- **Comprehensive latency & throughput metrics**:
  - Tracks enqueue, start, and finish timestamps per task.
  - Reports queue wait time (p50 / p99), execution time (p50 / p99), total latency, and throughput (tasks/s).
- **Load-balancing metrics** — per-worker atomic counters track task distribution without mutex contention.
- **Sanitizer-verified** — completely free of data races (TSan), memory leaks, and undefined behavior (ASan/UBSan).

## Architecture

```text
Producer threads -> bounded priority queue -> worker pool -> futures / results
                    (mutex + condition variables)
                         │
                    Dynamic Aging:
                    effective_priority = priority + (now - enqueued) / aging_interval
```

## Benchmark & Performance

Tested on an 8-core CPU (Apple M2) with Apple Clang 17.0 (`-O2 -pthread`). Results report the median of 3 runs (see detailed numbers in [`benchmarks/results.md`](benchmarks/results.md)):

### Workload 1: Coarse Tasks (~20 µs work per task, 20,000 tasks)

| Mode | Median Time | Throughput | Speedup | Queue Wait p50 |
|---|---:|---:|---:|---:|
| Sequential | 399.31 ms | 50,086 tasks/s | 1.00x | N/A |
| Pool — 1 worker | 500.06 ms | 39,995 tasks/s | 0.80x | 25,153 µs |
| Pool — 2 workers | 267.81 ms | 74,680 tasks/s | 1.49x | 13,284 µs |
| Pool — 4 workers | 134.58 ms | 148,608 tasks/s | 2.97x | 6,603 µs |
| Pool — 8 workers | 95.75 ms | 208,883 tasks/s | **4.17x** | **2,246 µs** |

### Workload 2: Fine Tasks (~1 µs work per task, 50,000 tasks)

| Mode | Median Time | Throughput | Speedup vs. Sequential |
|---|---:|---:|---:|
| Sequential | 16.58 ms | 3,016,159 tasks/s | 1.00x |
| Pool — 4 workers | 50.69 ms | 986,355 tasks/s | 0.33x |
| Pool — 8 workers | 97.59 ms | 512,340 tasks/s | 0.17x |

### Architectural Insights & Findings

1. **Why 8 workers yields ~4.17x and not 8.0x:**
   - **Core Heterogeneity:** The Apple M2 has 4 high-frequency Performance cores and 4 low-frequency Efficiency cores (which provide ~1/3 of the compute). Similarly, on x86 SMT/HyperThreading machines, 8 logical threads share 4 physical execution pipelines and L1/L2 caches, giving 20–30% boost per physical core rather than a 2x doubling.
   - **Lock Contention:** All workers contend for a single centralized `std::mutex` and condition variables; at high thread counts, cache-line bouncing increases lock acquisition latency.
   - **Amdahl's Law:** Serialized sections (enqueueing, aging checks, memory allocation, future resolution) cap maximum theoretical speedup.
2. **Why the thread pool loses on small (~1 µs) tasks:**
   - Synchronization overhead (mutex acquisition, condition variable wakeups, context switches, heap-allocated packaged tasks, and future state) costs ~2–4 µs per task.
   - When the payload is < 1 µs, synchronization overhead is several times larger than the work itself, while sequential execution runs with zero locking, zero context switches, and perfect L1 cache locality.

## Validation & Correctness

- **Concurrency correctness** — 1,000 submitted tasks complete correctly under high worker contention.
- **Strict priority & stable ordering** — higher-priority tasks execute before lower-priority queued tasks; identical priorities preserve FIFO order.
- **Aging & anti-starvation** — low-priority tasks elevate in effective priority while waiting, preventing starvation under continuous high-priority traffic.
- **Backpressure policies** — `try_submit` rejects immediately when full (`std::nullopt`); `submit_for` respects timeout deadlines.
- **Shutdown correctness** — `shutdown(Drain)` drains all queued work; `shutdown(Cancel)` discards pending tasks and cleanly sets futures to `broken_promise`.
- **Exception safety** — exceptions inside tasks propagate across thread boundaries through `future.get()`.
- **TSan-clean & ASan-clean** — zero data races and zero memory leaks.

## Build and Run

### Make

```bash
# Build all targets
make

# Run correctness test suite
make test

# Run benchmark suite (median of runs with latency p50/p99)
make benchmark

# Run ThreadSanitizer data race check
make tsan

# Run AddressSanitizer and UndefinedBehaviorSanitizer
make asan

# Clean build artifacts
make clean
```

### CMake

```bash
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
./build/benchmark
```

## Project Layout

```text
include/task_scheduler.hpp   Public scheduler interface, policies, and templates
src/task_scheduler.cpp       Worker pool, aging logic, metrics, and synchronization
src/benchmark.cpp            Multi-run benchmark harness (coarse vs. fine workloads)
tests/scheduler_tests.cpp    Exhaustive test suite (concurrency, aging, cancel, timeouts)
benchmarks/results.md        Reproducible benchmark measurements and detailed analysis
```

## Tech Stack

C++17 · `std::thread` · `std::mutex` · `std::condition_variable` · `std::future` · `std::packaged_task` · `std::chrono` · TSan · ASan · UBSan
