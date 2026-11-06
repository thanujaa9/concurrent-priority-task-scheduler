#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <vector>

// Shutdown policy when stopping the scheduler
enum class ShutdownMode {
    Drain,  // Finish all queued tasks before worker threads terminate (default)
    Cancel  // Discard pending queued tasks; worker threads terminate as soon as active tasks finish
};

// Comprehensive latency & throughput metrics
struct LatencyMetrics {
    std::size_t total_tasks{0};
    double queue_wait_p50_us{0.0};
    double queue_wait_p99_us{0.0};
    double execution_p50_us{0.0};
    double execution_p99_us{0.0};
    double total_latency_p50_us{0.0};
    double total_latency_p99_us{0.0};
    double throughput_tasks_per_sec{0.0};
};

// A bounded priority thread pool with aging, backpressure, and shutdown policies.
class TaskScheduler {
public:
    explicit TaskScheduler(
        std::size_t worker_count,
        std::size_t queue_capacity,
        std::chrono::milliseconds aging_interval = std::chrono::milliseconds(0));
    ~TaskScheduler();

    TaskScheduler(const TaskScheduler&) = delete;
    TaskScheduler& operator=(const TaskScheduler&) = delete;

    // Blocking submission: blocks when queue is full (default priority = 0)
    template <typename Function, typename... Args>
    auto submit(Function&& function, Args&&... args)
        -> std::future<typename std::invoke_result<Function, Args...>::type>;

    // Blocking submission with priority: higher priority runs first; FIFO on tie
    template <typename Function, typename... Args>
    auto submit_with_priority(int priority, Function&& function, Args&&... args)
        -> std::future<typename std::invoke_result<Function, Args...>::type>;

    // Non-blocking submission: returns std::nullopt immediately if queue is full
    template <typename Function, typename... Args>
    auto try_submit(Function&& function, Args&&... args)
        -> std::optional<std::future<typename std::invoke_result<Function, Args...>::type>>;

    template <typename Function, typename... Args>
    auto try_submit_with_priority(int priority, Function&& function, Args&&... args)
        -> std::optional<std::future<typename std::invoke_result<Function, Args...>::type>>;

    // Timed submission: waits up to timeout duration; returns std::nullopt if space remains unavailable
    template <typename Rep, typename Period, typename Function, typename... Args>
    auto submit_for(const std::chrono::duration<Rep, Period>& timeout, Function&& function, Args&&... args)
        -> std::optional<std::future<typename std::invoke_result<Function, Args...>::type>>;

    template <typename Rep, typename Period, typename Function, typename... Args>
    auto submit_with_priority_for(const std::chrono::duration<Rep, Period>& timeout, int priority, Function&& function, Args&&... args)
        -> std::optional<std::future<typename std::invoke_result<Function, Args...>::type>>;

    // Shutdown with selectable mode (Drain or Cancel). Safe to call multiple times.
    void shutdown(ShutdownMode mode = ShutdownMode::Drain);

    // Aging configuration: aging_interval > 0 prevents starvation by elevating older tasks
    std::chrono::milliseconds aging_interval() const noexcept;
    void set_aging_interval(std::chrono::milliseconds interval) noexcept;

    std::size_t worker_count() const noexcept;
    std::size_t queued_tasks() const;
    std::size_t completed_tasks() const noexcept;
    std::vector<std::size_t> tasks_per_worker() const;

    // Telemetry: p50/p99 wait & execution times, plus throughput
    LatencyMetrics latency_metrics() const;
    void reset_metrics();

private:
    struct QueuedTask {
        int priority;
        std::size_t sequence;
        std::chrono::steady_clock::time_point enqueued_at;
        std::function<void()> run;
    };

    struct TaskTimingRecord {
        std::chrono::steady_clock::time_point enqueued_at;
        std::chrono::steady_clock::time_point started_at;
        std::chrono::steady_clock::time_point finished_at;
    };

    void worker_loop(std::size_t worker_id);
    void enqueue_task_locked(int priority, std::function<void()> run);
    std::deque<QueuedTask>::iterator select_next_task_locked();

    const std::size_t queue_capacity_;
    std::atomic<std::int64_t> aging_interval_ms_{0};
    std::vector<std::thread> workers_;
    std::deque<QueuedTask> tasks_;
    mutable std::mutex mutex_;
    std::condition_variable task_available_;
    std::condition_variable space_available_;
    bool stopping_ = false;
    std::size_t next_sequence_ = 0;
    std::atomic<std::size_t> completed_tasks_{0};
    std::vector<std::atomic<std::size_t>> tasks_per_worker_;

    mutable std::mutex metrics_mutex_;
    std::vector<std::vector<TaskTimingRecord>> worker_timings_;
};

template <typename Function, typename... Args>
auto TaskScheduler::submit(Function&& function, Args&&... args)
    -> std::future<typename std::invoke_result<Function, Args...>::type> {
    return submit_with_priority(0, std::forward<Function>(function), std::forward<Args>(args)...);
}

template <typename Function, typename... Args>
auto TaskScheduler::submit_with_priority(int priority, Function&& function, Args&&... args)
    -> std::future<typename std::invoke_result<Function, Args...>::type> {
    using Result = typename std::invoke_result<Function, Args...>::type;

    std::unique_lock<std::mutex> lock(mutex_);
    space_available_.wait(lock, [this] {
        return stopping_ || tasks_.size() < queue_capacity_;
    });
    if (stopping_) {
        throw std::runtime_error("cannot submit work after shutdown");
    }

    auto task = std::make_shared<std::packaged_task<Result()>>(
        std::bind(std::forward<Function>(function), std::forward<Args>(args)...));
    std::future<Result> result = task->get_future();
    enqueue_task_locked(priority, [task] { (*task)(); });
    lock.unlock();
    task_available_.notify_one();
    return result;
}

template <typename Function, typename... Args>
auto TaskScheduler::try_submit(Function&& function, Args&&... args)
    -> std::optional<std::future<typename std::invoke_result<Function, Args...>::type>> {
    return try_submit_with_priority(0, std::forward<Function>(function), std::forward<Args>(args)...);
}

template <typename Function, typename... Args>
auto TaskScheduler::try_submit_with_priority(int priority, Function&& function, Args&&... args)
    -> std::optional<std::future<typename std::invoke_result<Function, Args...>::type>> {
    using Result = typename std::invoke_result<Function, Args...>::type;

    std::unique_lock<std::mutex> lock(mutex_);
    if (stopping_) {
        throw std::runtime_error("cannot submit work after shutdown");
    }
    if (tasks_.size() >= queue_capacity_) {
        return std::nullopt;
    }

    auto task = std::make_shared<std::packaged_task<Result()>>(
        std::bind(std::forward<Function>(function), std::forward<Args>(args)...));
    std::future<Result> result = task->get_future();
    enqueue_task_locked(priority, [task] { (*task)(); });
    lock.unlock();
    task_available_.notify_one();
    return result;
}

template <typename Rep, typename Period, typename Function, typename... Args>
auto TaskScheduler::submit_for(const std::chrono::duration<Rep, Period>& timeout, Function&& function, Args&&... args)
    -> std::optional<std::future<typename std::invoke_result<Function, Args...>::type>> {
    return submit_with_priority_for(timeout, 0, std::forward<Function>(function), std::forward<Args>(args)...);
}

template <typename Rep, typename Period, typename Function, typename... Args>
auto TaskScheduler::submit_with_priority_for(
    const std::chrono::duration<Rep, Period>& timeout,
    int priority,
    Function&& function,
    Args&&... args)
    -> std::optional<std::future<typename std::invoke_result<Function, Args...>::type>> {
    using Result = typename std::invoke_result<Function, Args...>::type;

    std::unique_lock<std::mutex> lock(mutex_);
    bool space = space_available_.wait_for(lock, timeout, [this] {
        return stopping_ || tasks_.size() < queue_capacity_;
    });
    if (stopping_) {
        throw std::runtime_error("cannot submit work after shutdown");
    }
    if (!space || tasks_.size() >= queue_capacity_) {
        return std::nullopt;
    }

    auto task = std::make_shared<std::packaged_task<Result()>>(
        std::bind(std::forward<Function>(function), std::forward<Args>(args)...));
    std::future<Result> result = task->get_future();
    enqueue_task_locked(priority, [task] { (*task)(); });
    lock.unlock();
    task_available_.notify_one();
    return result;
}
