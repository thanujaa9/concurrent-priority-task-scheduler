# Benchmark Results

Measured on a dedicated local machine. Each configuration was executed across three runs and the median is recorded below to eliminate transient system jitter.

## Test Environment

- **CPU Model:** Apple M2 (8 cores: 4 Performance "Avalanche" @ 3.5 GHz + 4 Efficiency "Blizzard" @ 2.4 GHz)
- **Physical Cores / Logical CPUs:** 8 physical cores / 8 logical CPUs
- **OS and Version:** macOS Darwin 24.6.0 (arm64)
- **Compiler and Version:** Apple clang version 17.0.0 (`clang-1700.3.19.1`)
- **Build Configuration / Flags:** `-std=c++17 -O2 -Wall -Wextra -Wpedantic -pthread -Iinclude`
- **Command:** `./build/benchmark`
- **Sanitizer Verification:** Fully verified clean with `-fsanitize=thread` (TSan) and `-fsanitize=address,undefined` (ASan/UBSan)
- **Date:** November 2026

---

## Results

### Workload 1: Coarse-Grained Tasks (Compute-Heavy)
*20,000 tasks, 10,000 iterations per task (~20–25 µs work payload per task)*

| Mode / Workers | Median Elapsed (ms) | Throughput (tasks/s) | Speedup vs. Sequential | Queue Wait p50 | Queue Wait p99 | Exec Time p50 |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| **Sequential Baseline** | **399.31 ms** | **50,086** | **1.00x** | N/A | N/A | ~20 µs |
| Pool — 1 worker | 500.06 ms | 39,995 | 0.80x | 25,153 µs | 28,523 µs | 24 µs |
| Pool — 2 workers | 267.81 ms | 74,680 | 1.49x | 13,284 µs | 16,703 µs | 26 µs |
| Pool — 4 workers | 134.58 ms | 148,608 | 2.97x | 6,603 µs | 9,677 µs | 20 µs |
| Pool — 8 workers | **95.75 ms** | **208,883** | **4.17x** | **2,246 µs** | **4,584 µs** | 20 µs |

*Work distribution across 8 workers:* `[2376, 2329, 2571, 2570, 2332, 2469, 2640, 2713]` (balanced load across all cores).

---

### Workload 2: Fine-Grained Tasks (Lightweight, ~1 µs)
*50,000 tasks, 200 iterations per task (~0.35–1.0 µs work payload per task)*

| Mode / Workers | Median Elapsed (ms) | Throughput (tasks/s) | Speedup vs. Sequential | Queue Wait p50 | Queue Wait p99 | Exec Time p50 |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| **Sequential Baseline** | **16.58 ms** | **3,016,159** | **1.00x** | N/A | N/A | ~0.35 µs |
| Pool — 1 worker | 124.57 ms | 401,373 | 0.13x | 2,490 µs | 2,845 µs | < 1 µs |
| Pool — 2 workers | 104.70 ms | 477,539 | 0.16x | 1,404 µs | 1,722 µs | < 1 µs |
| Pool — 4 workers | 50.69 ms | 986,355 | 0.33x | 11 µs | 53 µs | < 1 µs |
| Pool — 8 workers | 97.59 ms | 512,340 | 0.17x | 3 µs | 68 µs | < 1 µs |

---

## Detailed Findings & Performance Analysis

### 1. Why 8 Workers Yields ~4.17x Speedup Instead of 8.0x
In theory, an 8-core CPU might be expected to provide linear (8.0x) speedup for embarrassingly parallel workloads. In practice, speedup plateaus around 4.17x due to three architectural and concurrency factors:

1. **Core Heterogeneity (Apple Silicon P-cores vs. E-cores):**
   - The Apple M2 contains 4 Performance cores ("Avalanche") and 4 Efficiency cores ("Blizzard").
   - P-cores feature wide decode/execution pipelines and clock up to 3.5 GHz. E-cores clock at ~2.4 GHz with narrow pipelines designed for power efficiency, delivering roughly one-third of a P-core's IPC and throughput.
   - When 8 worker threads execute across all 8 cores, the 4 E-cores contribute significantly less compute than the 4 P-cores.
   - *(Note: On x86 architectures with Simultaneous Multi-Threading (SMT/HyperThreading), 8 logical CPUs on a 4-core machine share execution units and caches; SMT typically provides a 20–30% boost, never a 2x doubling).*

2. **Single Mutex & Condition Variable Contention:**
   - Every task submission by producer threads and every task pop by worker threads must acquire the scheduler's central `std::mutex`.
   - With 8 threads rapidly competing for the queue lock, cache-line bouncing occurs across CPU core caches, elevating lock acquisition latency.

3. **Amdahl’s Law and Serial Sections:**
   - Enqueueing tasks, calculating priorities/aging, dynamic memory allocations (`std::packaged_task`), and `std::future` promise resolutions constitute serialized overhead. Even a small serial fraction ($5\text{--}10\%$) places a mathematical upper bound on achievable parallel speedup.

---

### 2. Why the Thread Pool Loses to Sequential Execution on Fine-Grained Tasks
In Workload 2 (~1 µs task payload), the sequential loop completes in **16.58 ms** (~3 million tasks/second), whereas the 8-worker thread pool takes **97.59 ms** (a **0.17x slowdown**):

- **Synchronization Overhead Exceeds Work Payload:**
  - Pushing to a thread-safe queue, taking mutex locks, signaling `std::condition_variable`, executing context switches, allocating `std::packaged_task` on the heap, and establishing `std::future` state takes approximately **$2\text{--}4\ \mu\text{s}$ per task**.
  - When the actual task workload is only **$0.35\text{--}1.0\ \mu\text{s}$**, the synchronization overhead is **$3\text{--}10\times$ larger** than the work itself.
- **Cache Locality:**
  - Sequential execution runs tight in the CPU's L1 instruction and data cache with zero cache misses, zero context switches, and zero cross-core memory barriers.
- **Rule of Thumb:**
  - Tasks should generally have a compute duration $\ge 10\text{--}20\ \mu\text{s}$ (or be batched) to amortize thread pool synchronization overhead.
