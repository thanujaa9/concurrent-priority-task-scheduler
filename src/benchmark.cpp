#include "task_scheduler.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

using Clock = std::chrono::steady_clock;

static void cpu_work(std::size_t iterations) {
    volatile std::uint64_t value = 1;
    for (std::size_t i = 0; i < iterations; ++i) {
        value = value * 1664525ULL + 1013904223ULL;
    }
}

struct RunResult {
    double elapsed_ms{0.0};
    double throughput{0.0};
    double speedup{0.0};
    double wait_p50_us{0.0};
    double wait_p99_us{0.0};
    double exec_p50_us{0.0};
    double exec_p99_us{0.0};
    std::vector<std::size_t> distribution;
};

static double measure_sequential_median(std::size_t task_count, std::size_t iterations, int runs) {
    std::vector<double> samples;
    samples.reserve(runs);
    for (int r = 0; r < runs; ++r) {
        const auto start = Clock::now();
        for (std::size_t i = 0; i < task_count; ++i) {
            cpu_work(iterations);
        }
        double ms = std::chrono::duration<double, std::milli>(Clock::now() - start).count();
        samples.push_back(ms);
    }
    std::sort(samples.begin(), samples.end());
    return samples[samples.size() / 2];
}

static RunResult measure_pool_median(
    std::size_t threads,
    std::size_t queue_capacity,
    std::size_t task_count,
    std::size_t iterations,
    int runs,
    double baseline_ms) {
    std::vector<double> samples;
    samples.reserve(runs);
    std::vector<RunResult> run_results;
    run_results.reserve(runs);

    for (int r = 0; r < runs; ++r) {
        RunResult result;
        const auto start = Clock::now();
        {
            TaskScheduler scheduler(threads, queue_capacity);
            std::vector<std::future<void>> results;
            results.reserve(task_count);
            for (std::size_t i = 0; i < task_count; ++i) {
                results.push_back(scheduler.submit(cpu_work, iterations));
            }
            for (auto& res : results) {
                res.get();
            }
            result.distribution = scheduler.tasks_per_worker();
            auto metrics = scheduler.latency_metrics();
            result.wait_p50_us = metrics.queue_wait_p50_us;
            result.wait_p99_us = metrics.queue_wait_p99_us;
            result.exec_p50_us = metrics.execution_p50_us;
            result.exec_p99_us = metrics.execution_p99_us;
        }
        result.elapsed_ms = std::chrono::duration<double, std::milli>(Clock::now() - start).count();
        result.throughput = task_count * 1000.0 / result.elapsed_ms;
        result.speedup = baseline_ms / result.elapsed_ms;
        samples.push_back(result.elapsed_ms);
        run_results.push_back(result);
    }

    // Pick run with median elapsed time
    std::vector<std::size_t> indices(runs);
    for (int i = 0; i < runs; ++i) indices[i] = i;
    std::sort(indices.begin(), indices.end(), [&](std::size_t a, std::size_t b) {
        return samples[a] < samples[b];
    });
    return run_results[indices[runs / 2]];
}

static void run_benchmark_suite(
    const std::string& name,
    std::size_t task_count,
    std::size_t iterations,
    int runs,
    const std::vector<std::size_t>& thread_counts) {
    std::cout << "========================================================================================\n";
    std::cout << "Workload: " << name << " (tasks=" << task_count
              << ", work/task=" << iterations << " ops, runs=" << runs << ")\n";
    std::cout << "========================================================================================\n";

    double baseline_ms = measure_sequential_median(task_count, iterations, runs);
    double baseline_throughput = task_count * 1000.0 / baseline_ms;

    std::cout << "Sequential baseline (median): " << std::fixed << std::setprecision(2)
              << baseline_ms << " ms (" << std::setprecision(0) << baseline_throughput << " tasks/s)\n\n";

    std::cout << std::left
              << std::setw(10) << "Workers"
              << std::setw(14) << "Elapsed (ms)"
              << std::setw(18) << "Throughput (t/s)"
              << std::setw(10) << "Speedup"
              << std::setw(16) << "Queue Wait p50"
              << std::setw(16) << "Queue Wait p99"
              << std::setw(16) << "Exec Time p50"
              << "\n";
    std::cout << std::string(98, '-') << "\n";

    for (auto threads : thread_counts) {
        auto res = measure_pool_median(threads, 1024, task_count, iterations, runs, baseline_ms);
        std::ostringstream speedup_str;
        speedup_str << std::fixed << std::setprecision(2) << res.speedup << "x";

        std::cout << std::left
                  << std::setw(10) << threads
                  << std::setw(14) << std::fixed << std::setprecision(2) << res.elapsed_ms
                  << std::setw(18) << std::fixed << std::setprecision(0) << res.throughput
                  << std::setw(10) << speedup_str.str()
                  << std::setw(16) << (std::to_string((int)res.wait_p50_us) + " us")
                  << std::setw(16) << (std::to_string((int)res.wait_p99_us) + " us")
                  << std::setw(16) << (std::to_string((int)res.exec_p50_us) + " us")
                  << "\n";
        std::cout << "  Tasks per worker: [";
        for (std::size_t i = 0; i < res.distribution.size(); ++i) {
            std::cout << res.distribution[i] << (i + 1 == res.distribution.size() ? "" : ", ");
        }
        std::cout << "]\n";
    }
    std::cout << "\n";
}

int main(int argc, char** argv) {
    const int runs = 3;
    const auto hardware_threads = std::thread::hardware_concurrency();
    const std::vector<std::size_t> thread_counts = {1, 2, 4, 8};

    std::cout << "=== Concurrent Priority Task Scheduler Benchmark Suite ===\n";
    std::cout << "Logical hardware threads available: " << hardware_threads << "\n";
    std::cout << "Measurement strategy: Median of " << runs << " runs per configuration\n\n";

    if (argc > 2) {
        // Custom workload
        std::size_t task_count = std::strtoull(argv[1], nullptr, 10);
        std::size_t iterations = std::strtoull(argv[2], nullptr, 10);
        run_benchmark_suite("Custom Workload", task_count, iterations, runs, thread_counts);
    } else {
        // Workload 1: Coarse-grained tasks (~10-25 us work per task)
        run_benchmark_suite("Coarse Tasks (Compute-Heavy)", 20000, 10000, runs, thread_counts);

        // Workload 2: Fine-grained tasks (~1 us work per task)
        run_benchmark_suite("Fine Tasks (Lightweight ~1 us)", 50000, 200, runs, thread_counts);
    }

    std::cout << "Benchmark suite finished.\n";
    return 0;
}
