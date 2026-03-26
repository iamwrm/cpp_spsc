// SPSC Queue Throughput Benchmark
// Reproduces the throughput numbers from the analysis:
//   atomic_queue (SPSC=true)      ~662M msg/s
//   rigtorp::SPSCQueue            ~363K ops/ms
//   moodycamel::ReaderWriterQueue ~comparable
//   boost::lockfree::spsc_queue   ~210K ops/ms  (not included - needs boost)
//   std::mutex + std::queue       ~7K ops/ms
//
// Methodology: 1 producer thread, 1 consumer thread, pass uint64_t tokens
// through a bounded SPSC queue. Measure total wall-clock time for N ops.

#include <benchmark/benchmark.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <queue>
#include <semaphore>
#include <thread>

// ── Library headers ──
#include "readerwriterqueue.h"
#include "readerwritercircularbuffer.h"
#include "rigtorp/SPSCQueue.h"
#include "atomic_queue/atomic_queue.h"
#include "queue.hpp"  // ANDRVV/SPSCQueue

static constexpr size_t QUEUE_CAPACITY = 1024;

// ═══════════════════════════════════════════════════════════════════════════
// Helper: run a producer-consumer throughput test
// ═══════════════════════════════════════════════════════════════════════════
template <typename Enqueue, typename Dequeue>
void RunThroughput(benchmark::State& state, Enqueue enq, Dequeue deq) {
    const int64_t N = state.range(0);
    for (auto _ : state) {
        std::thread producer([&] {
            for (int64_t i = 0; i < N; ++i)
                enq(static_cast<uint64_t>(i));
        });
        std::thread consumer([&] {
            uint64_t val;
            for (int64_t i = 0; i < N; ++i)
                deq(val);
        });
        producer.join();
        consumer.join();
    }
    state.SetItemsProcessed(state.iterations() * N);
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. moodycamel::BlockingReaderWriterCircularBuffer (semaphore-backed)
// ═══════════════════════════════════════════════════════════════════════════
static void BM_Moodycamel_Blocking(benchmark::State& state) {
    moodycamel::BlockingReaderWriterCircularBuffer<uint64_t> q(QUEUE_CAPACITY);
    RunThroughput(state,
        [&](uint64_t v) { q.wait_enqueue(v); },
        [&](uint64_t& v) { q.wait_dequeue(v); });
}
BENCHMARK(BM_Moodycamel_Blocking)->Arg(1 << 20)->Unit(benchmark::kMillisecond);

// ═══════════════════════════════════════════════════════════════════════════
// 2. moodycamel::ReaderWriterQueue (try-based spin loop, unbounded variant)
// ═══════════════════════════════════════════════════════════════════════════
static void BM_Moodycamel_TrySpin(benchmark::State& state) {
    moodycamel::ReaderWriterQueue<uint64_t> q(QUEUE_CAPACITY);
    RunThroughput(state,
        [&](uint64_t v) { while (!q.try_enqueue(v)); },
        [&](uint64_t& v) { while (!q.try_dequeue(v)); });
}
BENCHMARK(BM_Moodycamel_TrySpin)->Arg(1 << 20)->Unit(benchmark::kMillisecond);

// ═══════════════════════════════════════════════════════════════════════════
// 3. rigtorp::SPSCQueue (spin-only)
// ═══════════════════════════════════════════════════════════════════════════
static void BM_Rigtorp(benchmark::State& state) {
    rigtorp::SPSCQueue<uint64_t> q(QUEUE_CAPACITY);
    RunThroughput(state,
        [&](uint64_t v) { while (!q.try_push(v)); },
        [&](uint64_t& v) {
            uint64_t* p;
            while (!(p = q.front()));
            v = *p;
            q.pop();
        });
}
BENCHMARK(BM_Rigtorp)->Arg(1 << 20)->Unit(benchmark::kMillisecond);

// ═══════════════════════════════════════════════════════════════════════════
// 4. atomic_queue (SPSC=true)
// ═══════════════════════════════════════════════════════════════════════════
static void BM_AtomicQueue(benchmark::State& state) {
    // AtomicQueue2 uses unsigned NIL element; we use 0xFFFF... as NIL
    // so valid values are 0..N which won't collide
    atomic_queue::AtomicQueue2<uint64_t, QUEUE_CAPACITY, true> q;
    RunThroughput(state,
        [&](uint64_t v) { q.push(v); },
        [&](uint64_t& v) { v = q.pop(); });
}
BENCHMARK(BM_AtomicQueue)->Arg(1 << 20)->Unit(benchmark::kMillisecond);

// ═══════════════════════════════════════════════════════════════════════════
// 5. ANDRVV/SPSCQueue (spin-only, blocking push/pop)
// ═══════════════════════════════════════════════════════════════════════════
static void BM_ANDRVV(benchmark::State& state) {
    SPSCQueue<uint64_t> q(QUEUE_CAPACITY);
    RunThroughput(state,
        [&](uint64_t v) { q.push(v); },
        [&](uint64_t& v) { v = q.pop(); });
}
BENCHMARK(BM_ANDRVV)->Arg(1 << 20)->Unit(benchmark::kMillisecond);

// ═══════════════════════════════════════════════════════════════════════════
// 6. C++20 DIY: rigtorp + std::counting_semaphore
// ═══════════════════════════════════════════════════════════════════════════
template <typename T, size_t N>
struct Channel {
    rigtorp::SPSCQueue<T> q{N};
    std::counting_semaphore<N> free{N};
    std::counting_semaphore<N> ready{0};

    void send(T val) { free.acquire(); while (!q.try_push(val)); ready.release(); }
    T recv()         { ready.acquire(); T v = *q.front(); q.pop(); free.release(); return v; }
};

static void BM_DIY_Channel(benchmark::State& state) {
    Channel<uint64_t, QUEUE_CAPACITY> ch;
    const int64_t N = state.range(0);
    for (auto _ : state) {
        std::thread producer([&] {
            for (int64_t i = 0; i < N; ++i)
                ch.send(static_cast<uint64_t>(i));
        });
        std::thread consumer([&] {
            for (int64_t i = 0; i < N; ++i)
                benchmark::DoNotOptimize(ch.recv());
        });
        producer.join();
        consumer.join();
    }
    state.SetItemsProcessed(state.iterations() * N);
}
BENCHMARK(BM_DIY_Channel)->Arg(1 << 20)->Unit(benchmark::kMillisecond);

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

static void BM_MutexQueue(benchmark::State& state) {
    MutexQueue<uint64_t, QUEUE_CAPACITY> q;
    const int64_t N = state.range(0);
    for (auto _ : state) {
        std::thread producer([&] {
            for (int64_t i = 0; i < N; ++i)
                q.push(static_cast<uint64_t>(i));
        });
        std::thread consumer([&] {
            for (int64_t i = 0; i < N; ++i)
                benchmark::DoNotOptimize(q.pop());
        });
        producer.join();
        consumer.join();
    }
    state.SetItemsProcessed(state.iterations() * N);
}
BENCHMARK(BM_MutexQueue)->Arg(1 << 20)->Unit(benchmark::kMillisecond);

BENCHMARK_MAIN();
