// SPSC Queue Round-Trip Latency (RTT) Benchmark
// Measures ping-pong latency: producer sends, consumer echoes back.
// Reports median and percentile RTT in nanoseconds.
//
// Expected results from the analysis:
//   rigtorp::SPSCQueue            ~133ns
//   boost::lockfree::spsc_queue   ~132ns  (not included)
//   moodycamel::ReaderWriterQueue ~137ns
//   atomic_queue (SPSC=true)      ~153ns
//   std::mutex + std::queue       ~2340ns

#include <atomic>
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <numeric>
#include <queue>
#include <semaphore>
#include <thread>
#include <vector>

#include "readerwriterqueue.h"
#include "readerwritercircularbuffer.h"
#include "rigtorp/SPSCQueue.h"
#include "atomic_queue/atomic_queue.h"

static constexpr size_t QUEUE_CAPACITY = 1024;
static constexpr int WARMUP = 1000;
static constexpr int ITERS  = 100000;

using Clock = std::chrono::high_resolution_clock;

// ═══════════════════════════════════════════════════════════════════════════
// Report percentiles
// ═══════════════════════════════════════════════════════════════════════════
void report(const char* name, std::vector<int64_t>& latencies) {
    std::sort(latencies.begin(), latencies.end());
    auto pct = [&](double p) -> int64_t {
        size_t idx = static_cast<size_t>(p * latencies.size());
        if (idx >= latencies.size()) idx = latencies.size() - 1;
        return latencies[idx];
    };
    double mean = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();

    std::printf("%-42s  mean=%6.0fns  p50=%5ldns  p90=%5ldns  p99=%5ldns  p99.9=%5ldns\n",
                name, mean, pct(0.50), pct(0.90), pct(0.99), pct(0.999));
}

// ═══════════════════════════════════════════════════════════════════════════
// Generic ping-pong RTT measurement
// Producer -> Q_fwd -> Consumer -> Q_back -> Producer
// ═══════════════════════════════════════════════════════════════════════════
template <typename SendFwd, typename RecvFwd, typename SendBack, typename RecvBack>
void measure_rtt(const char* name,
                 SendFwd send_fwd, RecvFwd recv_fwd,
                 SendBack send_back, RecvBack recv_back) {
    std::vector<int64_t> latencies;
    latencies.reserve(ITERS);

    std::atomic<bool> start{false};

    // Consumer: echo back
    std::thread consumer([&] {
        while (!start.load(std::memory_order_acquire));
        for (int i = 0; i < WARMUP + ITERS; ++i) {
            uint64_t v = recv_fwd();
            send_back(v);
        }
    });

    // Producer: send and time round-trip
    start.store(true, std::memory_order_release);
    for (int i = 0; i < WARMUP + ITERS; ++i) {
        auto t0 = Clock::now();
        send_fwd(static_cast<uint64_t>(i));
        uint64_t v = recv_back();
        auto t1 = Clock::now();
        (void)v;
        if (i >= WARMUP) {
            latencies.push_back(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
        }
    }
    consumer.join();
    report(name, latencies);
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. rigtorp::SPSCQueue
// ═══════════════════════════════════════════════════════════════════════════
void bench_rigtorp() {
    rigtorp::SPSCQueue<uint64_t> fwd(QUEUE_CAPACITY);
    rigtorp::SPSCQueue<uint64_t> back(QUEUE_CAPACITY);
    measure_rtt("rigtorp::SPSCQueue",
        [&](uint64_t v) { while (!fwd.try_push(v)); },
        [&]() -> uint64_t { uint64_t* p; while (!(p = fwd.front())); uint64_t v = *p; fwd.pop(); return v; },
        [&](uint64_t v) { while (!back.try_push(v)); },
        [&]() -> uint64_t { uint64_t* p; while (!(p = back.front())); uint64_t v = *p; back.pop(); return v; });
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. moodycamel::ReaderWriterQueue (try-spin)
// ═══════════════════════════════════════════════════════════════════════════
void bench_moodycamel_spin() {
    moodycamel::ReaderWriterQueue<uint64_t> fwd(QUEUE_CAPACITY);
    moodycamel::ReaderWriterQueue<uint64_t> back(QUEUE_CAPACITY);
    measure_rtt("moodycamel::ReaderWriterQueue (try-spin)",
        [&](uint64_t v) { while (!fwd.try_enqueue(v)); },
        [&]() -> uint64_t { uint64_t v; while (!fwd.try_dequeue(v)); return v; },
        [&](uint64_t v) { while (!back.try_enqueue(v)); },
        [&]() -> uint64_t { uint64_t v; while (!back.try_dequeue(v)); return v; });
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. moodycamel::BlockingReaderWriterCircularBuffer (semaphore-backed)
// ═══════════════════════════════════════════════════════════════════════════
void bench_moodycamel_blocking() {
    moodycamel::BlockingReaderWriterCircularBuffer<uint64_t> fwd(QUEUE_CAPACITY);
    moodycamel::BlockingReaderWriterCircularBuffer<uint64_t> back(QUEUE_CAPACITY);
    measure_rtt("moodycamel::BlockingCircularBuffer",
        [&](uint64_t v) { fwd.wait_enqueue(v); },
        [&]() -> uint64_t { uint64_t v; fwd.wait_dequeue(v); return v; },
        [&](uint64_t v) { back.wait_enqueue(v); },
        [&]() -> uint64_t { uint64_t v; back.wait_dequeue(v); return v; });
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. atomic_queue (SPSC=true)
// ═══════════════════════════════════════════════════════════════════════════
void bench_atomic_queue() {
    atomic_queue::AtomicQueue2<uint64_t, QUEUE_CAPACITY, true> fwd;
    atomic_queue::AtomicQueue2<uint64_t, QUEUE_CAPACITY, true> back;
    measure_rtt("atomic_queue::AtomicQueue2 (SPSC=true)",
        [&](uint64_t v) { fwd.push(v); },
        [&]() -> uint64_t { return fwd.pop(); },
        [&](uint64_t v) { back.push(v); },
        [&]() -> uint64_t { return back.pop(); });
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. C++20 DIY Channel (rigtorp + counting_semaphore)
// ═══════════════════════════════════════════════════════════════════════════
template <typename T, size_t N>
struct Channel {
    rigtorp::SPSCQueue<T> q{N};
    std::counting_semaphore<N> free{N};
    std::counting_semaphore<N> ready{0};

    void send(T val) { free.acquire(); while (!q.try_push(val)); ready.release(); }
    T recv()         { ready.acquire(); T v = *q.front(); q.pop(); free.release(); return v; }
};

void bench_diy_channel() {
    Channel<uint64_t, QUEUE_CAPACITY> fwd;
    Channel<uint64_t, QUEUE_CAPACITY> back;
    measure_rtt("DIY Channel (rigtorp + semaphore)",
        [&](uint64_t v) { fwd.send(v); },
        [&]() -> uint64_t { return fwd.recv(); },
        [&](uint64_t v) { back.send(v); },
        [&]() -> uint64_t { return back.recv(); });
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. std::mutex + std::queue (baseline)
// ═══════════════════════════════════════════════════════════════════════════
template <typename T, size_t Cap>
struct MutexQueue {
    std::queue<T> q;
    std::mutex mtx;
    std::condition_variable cv_not_full;
    std::condition_variable cv_not_empty;

    void push(T val) {
        std::unique_lock lk(mtx);
        cv_not_full.wait(lk, [&] { return q.size() < Cap; });
        q.push(val);
        lk.unlock();
        cv_not_empty.notify_one();
    }
    T pop() {
        std::unique_lock lk(mtx);
        cv_not_empty.wait(lk, [&] { return !q.empty(); });
        T val = q.front();
        q.pop();
        lk.unlock();
        cv_not_full.notify_one();
        return val;
    }
};

void bench_mutex_queue() {
    MutexQueue<uint64_t, QUEUE_CAPACITY> fwd;
    MutexQueue<uint64_t, QUEUE_CAPACITY> back;
    measure_rtt("std::mutex + std::queue",
        [&](uint64_t v) { fwd.push(v); },
        [&]() -> uint64_t { return fwd.pop(); },
        [&](uint64_t v) { back.push(v); },
        [&]() -> uint64_t { return back.pop(); });
}

// ═══════════════════════════════════════════════════════════════════════════
int main() {
    std::printf("SPSC Round-Trip Latency Benchmark\n");
    std::printf("  Queue capacity: %zu, Warmup: %d, Iterations: %d\n\n", QUEUE_CAPACITY, WARMUP, ITERS);
    std::printf("%-42s  %10s  %9s  %9s  %9s  %11s\n",
                "Queue", "mean", "p50", "p90", "p99", "p99.9");
    std::printf("%s\n", std::string(120, '-').c_str());

    bench_rigtorp();
    bench_moodycamel_spin();
    bench_moodycamel_blocking();
    bench_atomic_queue();
    bench_diy_channel();
    bench_mutex_queue();

    return 0;
}
