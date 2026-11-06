#include "task_scheduler.hpp"

#include <algorithm>
#include <stdexcept>

TaskScheduler::TaskScheduler(
    std::size_t worker_count,
    std::size_t queue_capacity,
    std::chrono::milliseconds aging_interval)
    : queue_capacity_(queue_capacity),
      aging_interval_ms_(aging_interval.count()) {
    if (worker_count == 0 || queue_capacity == 0) {
        throw std::invalid_argument("worker count and queue capacity must be positive");
    }
    workers_.reserve(worker_count);
    tasks_per_worker_ = std::vector<std::atomic<std::size_t>>(worker_count);
    worker_timings_.resize(worker_count);
    for (std::size_t i = 0; i < worker_count; ++i) {
        workers_.emplace_back(&TaskScheduler::worker_loop, this, i);
    }
}

TaskScheduler::~TaskScheduler() {
    shutdown(ShutdownMode::Drain);
}

void TaskScheduler::shutdown(ShutdownMode mode) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) {
            if (mode == ShutdownMode::Cancel && !tasks_.empty()) {
                tasks_.clear();
                task_available_.notify_all();
                space_available_.notify_all();
            }
            return;
        }
        stopping_ = true;
        if (mode == ShutdownMode::Cancel) {
            tasks_.clear();
        }
    }
    task_available_.notify_all();
    space_available_.notify_all();
    for (auto& worker : workers_) {
        if (worker.joinable() && worker.get_id() != std::this_thread::get_id()) {
            worker.join();
        }
    }
}

std::chrono::milliseconds TaskScheduler::aging_interval() const noexcept {
    return std::chrono::milliseconds(aging_interval_ms_.load(std::memory_order_relaxed));
}

void TaskScheduler::set_aging_interval(std::chrono::milliseconds interval) noexcept {
    aging_interval_ms_.store(interval.count(), std::memory_order_relaxed);
}

std::size_t TaskScheduler::worker_count() const noexcept {
    return workers_.size();
}

std::size_t TaskScheduler::queued_tasks() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return tasks_.size();
}

std::size_t TaskScheduler::completed_tasks() const noexcept {
    return completed_tasks_.load(std::memory_order_relaxed);
}

std::vector<std::size_t> TaskScheduler::tasks_per_worker() const {
    std::vector<std::size_t> counts;
    counts.reserve(tasks_per_worker_.size());
    for (const auto& count : tasks_per_worker_) {
        counts.push_back(count.load(std::memory_order_relaxed));
    }
    return counts;
}

void TaskScheduler::enqueue_task_locked(int priority, std::function<void()> run) {
    QueuedTask queued{
        priority,
        next_sequence_++,
        std::chrono::steady_clock::now(),
        std::move(run)
    };
    auto position = tasks_.begin();
    while (position != tasks_.end() && position->priority >= priority) {
        ++position;
    }
    tasks_.insert(position, std::move(queued));
}

std::deque<TaskScheduler::QueuedTask>::iterator TaskScheduler::select_next_task_locked() {
    auto aging_ms = aging_interval_ms_.load(std::memory_order_relaxed);
    if (aging_ms <= 0 || tasks_.size() <= 1) {
        return tasks_.begin();
    }
    auto now = std::chrono::steady_clock::now();
    auto best_it = tasks_.begin();
    auto calc_effective = [&](const QueuedTask& t) {
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - t.enqueued_at).count();
        return t.priority + static_cast<int>(elapsed_ms / aging_ms);
    };

    int best_effective = calc_effective(*best_it);
    for (auto it = std::next(tasks_.begin()); it != tasks_.end(); ++it) {
        int eff = calc_effective(*it);
        // Higher effective priority wins.
        // Tie-breaker: earlier sequence number (FIFO order for equal effective priority).
        if (eff > best_effective || (eff == best_effective && it->sequence < best_it->sequence)) {
            best_it = it;
            best_effective = eff;
        }
    }
    return best_it;
}

void TaskScheduler::worker_loop(std::size_t worker_id) {
    while (true) {
        std::function<void()> task;
        std::chrono::steady_clock::time_point enqueued_at;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            task_available_.wait(lock, [this] { return stopping_ || !tasks_.empty(); });
            if (stopping_ && tasks_.empty()) return;

            auto it = select_next_task_locked();
            enqueued_at = it->enqueued_at;
            task = std::move(it->run);
            tasks_.erase(it);
        }
        space_available_.notify_one();

        auto started_at = std::chrono::steady_clock::now();
        task();
        auto finished_at = std::chrono::steady_clock::now();

        completed_tasks_.fetch_add(1, std::memory_order_relaxed);
        tasks_per_worker_[worker_id].fetch_add(1, std::memory_order_relaxed);

        {
            std::lock_guard<std::mutex> lock(metrics_mutex_);
            worker_timings_[worker_id].push_back({enqueued_at, started_at, finished_at});
        }
    }
}

LatencyMetrics TaskScheduler::latency_metrics() const {
    std::vector<TaskTimingRecord> all_timings;
    {
        std::lock_guard<std::mutex> lock(metrics_mutex_);
        std::size_t total = 0;
        for (const auto& vec : worker_timings_) total += vec.size();
        all_timings.reserve(total);
        for (const auto& vec : worker_timings_) {
            all_timings.insert(all_timings.end(), vec.begin(), vec.end());
        }
    }

    LatencyMetrics metrics;
    metrics.total_tasks = all_timings.size();
    if (all_timings.empty()) {
        return metrics;
    }

    std::vector<double> queue_waits_us;
    std::vector<double> exec_times_us;
    std::vector<double> total_latencies_us;
    queue_waits_us.reserve(all_timings.size());
    exec_times_us.reserve(all_timings.size());
    total_latencies_us.reserve(all_timings.size());

    auto earliest_start = all_timings.front().started_at;
    auto latest_finish = all_timings.front().finished_at;

    for (const auto& t : all_timings) {
        double wait_us = std::chrono::duration<double, std::micro>(t.started_at - t.enqueued_at).count();
        double exec_us = std::chrono::duration<double, std::micro>(t.finished_at - t.started_at).count();
        double total_us = std::chrono::duration<double, std::micro>(t.finished_at - t.enqueued_at).count();

        queue_waits_us.push_back(wait_us < 0.0 ? 0.0 : wait_us);
        exec_times_us.push_back(exec_us < 0.0 ? 0.0 : exec_us);
        total_latencies_us.push_back(total_us < 0.0 ? 0.0 : total_us);

        if (t.started_at < earliest_start) earliest_start = t.started_at;
        if (t.finished_at > latest_finish) latest_finish = t.finished_at;
    }

    auto calc_percentile = [](std::vector<double>& v, double p) -> double {
        if (v.empty()) return 0.0;
        std::size_t idx = static_cast<std::size_t>(v.size() * p);
        if (idx >= v.size()) idx = v.size() - 1;
        std::nth_element(v.begin(), v.begin() + idx, v.end());
        return v[idx];
    };

    metrics.queue_wait_p50_us = calc_percentile(queue_waits_us, 0.50);
    metrics.queue_wait_p99_us = calc_percentile(queue_waits_us, 0.99);
    metrics.execution_p50_us = calc_percentile(exec_times_us, 0.50);
    metrics.execution_p99_us = calc_percentile(exec_times_us, 0.99);
    metrics.total_latency_p50_us = calc_percentile(total_latencies_us, 0.50);
    metrics.total_latency_p99_us = calc_percentile(total_latencies_us, 0.99);

    double total_wall_sec = std::chrono::duration<double>(latest_finish - earliest_start).count();
    if (total_wall_sec > 1e-9) {
        metrics.throughput_tasks_per_sec = metrics.total_tasks / total_wall_sec;
    } else {
        metrics.throughput_tasks_per_sec = 0.0;
    }

    return metrics;
}

void TaskScheduler::reset_metrics() {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    for (auto& vec : worker_timings_) {
        vec.clear();
    }
}
