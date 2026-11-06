#include "task_scheduler.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>

void test_basic_concurrency_and_metrics() {
    std::cout << "Testing basic concurrency and metrics...\n";
    TaskScheduler scheduler(4, 8);
    std::atomic<int> sum{0};
    std::vector<std::future<void>> tasks;
    for (int i = 0; i < 1000; ++i) {
        tasks.push_back(scheduler.submit([&sum] { sum.fetch_add(1, std::memory_order_relaxed); }));
    }
    for (auto& task : tasks) task.get();
    assert(sum == 1000);
    assert(scheduler.completed_tasks() == 1000);

    auto counts = scheduler.tasks_per_worker();
    int distributed = 0;
    for (auto count : counts) distributed += static_cast<int>(count);
    assert(distributed == 1000);

    auto metrics = scheduler.latency_metrics();
    assert(metrics.total_tasks == 1000);
    assert(metrics.queue_wait_p50_us >= 0.0);
    assert(metrics.queue_wait_p99_us >= metrics.queue_wait_p50_us);
    assert(metrics.execution_p50_us >= 0.0);
    assert(metrics.execution_p99_us >= metrics.execution_p50_us);
    assert(metrics.throughput_tasks_per_sec > 0.0);

    scheduler.reset_metrics();
    assert(scheduler.latency_metrics().total_tasks == 0);

    auto value = scheduler.submit([] { return 42; });
    assert(value.get() == 42);

    auto failure = scheduler.submit([]() -> int { throw std::runtime_error("expected"); });
    try {
        failure.get();
        assert(false);
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()) == "expected");
    }

    scheduler.shutdown();
    try {
        scheduler.submit([] {});
        assert(false);
    } catch (const std::runtime_error&) {}
}

void test_priority_ordering() {
    std::cout << "Testing strict priority ordering...\n";
    TaskScheduler priority_scheduler(1, 4);
    std::mutex gate_mutex;
    std::condition_variable gate;
    bool release_worker = false;
    std::promise<void> blocker_running;
    std::vector<int> run_order;

    auto blocker = priority_scheduler.submit([&] {
        blocker_running.set_value();
        std::unique_lock<std::mutex> lock(gate_mutex);
        gate.wait(lock, [&] { return release_worker; });
    });

    blocker_running.get_future().wait();

    auto low = priority_scheduler.submit_with_priority(1, [&] { run_order.push_back(1); });
    auto high = priority_scheduler.submit_with_priority(10, [&] { run_order.push_back(10); });

    {
        std::lock_guard<std::mutex> lock(gate_mutex);
        release_worker = true;
    }
    gate.notify_one();
    blocker.get();
    low.get();
    high.get();
    assert((run_order == std::vector<int>{10, 1}));
    priority_scheduler.shutdown();
}

void test_aging_prevents_starvation() {
    std::cout << "Testing aging prevents starvation...\n";
    // 1 worker, queue capacity 32, aging interval 10ms.
    // Every 10ms of waiting gives +1 priority.
    TaskScheduler scheduler(1, 32, std::chrono::milliseconds(10));

    std::mutex block_mutex;
    std::condition_variable block_cv;
    bool allow_worker = false;
    std::promise<void> blocker_running;

    // Block the worker temporarily so tasks accumulate
    auto blocker = scheduler.submit([&] {
        blocker_running.set_value();
        std::unique_lock<std::mutex> lock(block_mutex);
        block_cv.wait(lock, [&] { return allow_worker; });
    });

    blocker_running.get_future().wait();

    std::vector<int> execution_order;
    std::mutex order_mutex;

    // Submit a low-priority task (priority 0)
    auto low_task = scheduler.submit_with_priority(0, [&] {
        std::lock_guard<std::mutex> lock(order_mutex);
        execution_order.push_back(0);
    });

    // Wait 60ms so low_task ages by ~6 priority levels (effective priority >= 6)
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    // Submit higher priority task (priority 5) after the wait
    auto medium_task = scheduler.submit_with_priority(5, [&] {
        std::lock_guard<std::mutex> lock(order_mutex);
        execution_order.push_back(5);
    });

    // Unblock the worker
    {
        std::lock_guard<std::mutex> lock(block_mutex);
        allow_worker = true;
    }
    block_cv.notify_one();

    blocker.get();
    low_task.get();
    medium_task.get();

    // Because low_task aged for ~60ms with a 10ms aging interval,
    // its effective priority became 0 + 6 = 6, which is > 5.
    // Therefore, low_task (0) runs BEFORE medium_task (5)!
    {
        std::lock_guard<std::mutex> lock(order_mutex);
        assert(execution_order.size() == 2);
        assert(execution_order[0] == 0);
        assert(execution_order[1] == 5);
    }
    scheduler.shutdown();
}

void test_try_submit_backpressure() {
    std::cout << "Testing try_submit (non-blocking reject)...\n";
    TaskScheduler scheduler(1, 2);

    std::mutex block_mutex;
    std::condition_variable block_cv;
    bool allow_worker = false;
    std::promise<void> blocker_running;

    // Worker holds the running task
    auto blocker = scheduler.submit([&] {
        blocker_running.set_value();
        std::unique_lock<std::mutex> lock(block_mutex);
        block_cv.wait(lock, [&] { return allow_worker; });
    });

    // Wait until blocker has been popped from queue and is actively executing
    blocker_running.get_future().wait();

    // Queue has capacity 2: fill it
    auto f1 = scheduler.try_submit([] { return 1; });
    auto f2 = scheduler.try_submit([] { return 2; });
    assert(f1.has_value());
    assert(f2.has_value());

    // Queue is full now (capacity 2 reached)
    auto f3 = scheduler.try_submit([] { return 3; });
    assert(!f3.has_value()); // Rejected without blocking

    // Release worker
    {
        std::lock_guard<std::mutex> lock(block_mutex);
        allow_worker = true;
    }
    block_cv.notify_one();

    blocker.get();
    assert(f1->get() == 1);
    assert(f2->get() == 2);

    // Now space is available, try_submit succeeds
    auto f4 = scheduler.try_submit([] { return 4; });
    assert(f4.has_value());
    assert(f4->get() == 4);

    scheduler.shutdown();
}

void test_submit_for_timeout() {
    std::cout << "Testing submit_for (timeout backpressure)...\n";
    TaskScheduler scheduler(1, 1);

    std::mutex block_mutex;
    std::condition_variable block_cv;
    bool allow_worker = false;
    std::promise<void> blocker_running;

    auto blocker = scheduler.submit([&] {
        blocker_running.set_value();
        std::unique_lock<std::mutex> lock(block_mutex);
        block_cv.wait(lock, [&] { return allow_worker; });
    });

    // Ensure blocker is popped from queue
    blocker_running.get_future().wait();

    // Fill the queue (capacity 1)
    auto f1 = scheduler.submit([] { return 10; });

    // Queue is full: submit_for with 30ms timeout should time out
    auto start = std::chrono::steady_clock::now();
    auto timed_out = scheduler.submit_for(std::chrono::milliseconds(30), [] { return 20; });
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
    assert(!timed_out.has_value());
    assert(elapsed.count() >= 25);

    // Now submit with 500ms timeout, but unblock the worker after 40ms
    std::thread unblocker([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(40));
        {
            std::lock_guard<std::mutex> lock(block_mutex);
            allow_worker = true;
        }
        block_cv.notify_one();
    });

    auto succeeded = scheduler.submit_for(std::chrono::milliseconds(500), [] { return 30; });
    assert(succeeded.has_value());
    unblocker.join();

    blocker.get();
    assert(f1.get() == 10);
    assert(succeeded->get() == 30);

    scheduler.shutdown();
}

void test_shutdown_modes() {
    std::cout << "Testing shutdown(Drain) vs shutdown(Cancel)...\n";

    // 1. Drain: all queued tasks must complete
    {
        TaskScheduler scheduler(2, 10);
        std::atomic<int> counter{0};
        for (int i = 0; i < 8; ++i) {
            scheduler.submit([&counter] {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
                counter.fetch_add(1, std::memory_order_relaxed);
            });
        }
        scheduler.shutdown(ShutdownMode::Drain);
        assert(counter.load() == 8);
    }

    // 2. Cancel: pending queued tasks are dropped and their futures throw broken_promise
    {
        TaskScheduler scheduler(1, 10);
        std::mutex block_mutex;
        std::condition_variable block_cv;
        bool allow_worker = false;
        std::promise<void> blocker_running;

        auto blocker = scheduler.submit([&] {
            blocker_running.set_value();
            std::unique_lock<std::mutex> lock(block_mutex);
            block_cv.wait(lock, [&] { return allow_worker; });
        });

        // Ensure blocker is running
        blocker_running.get_future().wait();

        std::vector<std::future<int>> pending_futures;
        for (int i = 0; i < 5; ++i) {
            pending_futures.push_back(scheduler.submit([i] { return i; }));
        }

        // Trigger cancel shutdown in background thread while worker is still blocked
        std::thread cancel_thread([&scheduler] {
            scheduler.shutdown(ShutdownMode::Cancel);
        });

        // Let the worker finish its active blocker task
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        {
            std::lock_guard<std::mutex> lock(block_mutex);
            allow_worker = true;
        }
        block_cv.notify_one();

        blocker.get();
        cancel_thread.join();

        // All pending tasks should have had their futures broken/cancelled
        int broken_count = 0;
        for (auto& fut : pending_futures) {
            try {
                fut.get();
            } catch (const std::future_error& e) {
                if (e.code() == std::future_errc::broken_promise) {
                    ++broken_count;
                }
            }
        }
        assert(broken_count == 5);
    }
}

int main() {
    test_basic_concurrency_and_metrics();
    test_priority_ordering();
    test_aging_prevents_starvation();
    test_try_submit_backpressure();
    test_submit_for_timeout();
    test_shutdown_modes();
    std::cout << "\n>>> All scheduler tests passed successfully! <<<\n";
    return 0;
}
