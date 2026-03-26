cmake_minimum_required(VERSION 3.20)
project(spsc_benchmark CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(FetchContent)

# ── moodycamel::ReaderWriterQueue (includes BlockingReaderWriterCircularBuffer) ──
FetchContent_Declare(
    readerwriterqueue
    GIT_REPOSITORY https://github.com/cameron314/readerwriterqueue.git
    GIT_TAG        v1.0.6
    GIT_SHALLOW    TRUE
)

# ── rigtorp::SPSCQueue ──
FetchContent_Declare(
    spscqueue
    GIT_REPOSITORY https://github.com/rigtorp/SPSCQueue.git
    GIT_TAG        v1.1
    GIT_SHALLOW    TRUE
)

# ── max0x7ba::atomic_queue ──
FetchContent_Declare(
    atomic_queue
    GIT_REPOSITORY https://github.com/max0x7ba/atomic_queue.git
    GIT_TAG        master
    GIT_SHALLOW    TRUE
)

# ── ANDRVV/SPSCQueue ──
FetchContent_Declare(
    andrvv_spscqueue
    GIT_REPOSITORY https://github.com/ANDRVV/SPSCQueue.git
    GIT_TAG        main
    GIT_SHALLOW    TRUE
)

# ── Google Benchmark ──
FetchContent_Declare(
    benchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG        v1.9.1
    GIT_SHALLOW    TRUE
)
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(readerwriterqueue spscqueue atomic_queue andrvv_spscqueue benchmark)

# ── Throughput benchmark ──
add_executable(spsc_bench bench/spsc_bench.cpp)
target_include_directories(spsc_bench PRIVATE
    ${readerwriterqueue_SOURCE_DIR}
    ${spscqueue_SOURCE_DIR}/include
    ${atomic_queue_SOURCE_DIR}/include
    "${andrvv_spscqueue_SOURCE_DIR}/src/c++"
)
target_link_libraries(spsc_bench PRIVATE benchmark::benchmark pthread)
target_compile_options(spsc_bench PRIVATE -O3 -march=native -DNDEBUG)

# ── Latency (RTT) benchmark ──
add_executable(spsc_latency bench/spsc_latency.cpp)
target_include_directories(spsc_latency PRIVATE
    ${readerwriterqueue_SOURCE_DIR}
    ${spscqueue_SOURCE_DIR}/include
    ${atomic_queue_SOURCE_DIR}/include
    "${andrvv_spscqueue_SOURCE_DIR}/src/c++"
)
target_link_libraries(spsc_latency PRIVATE pthread)
target_compile_options(spsc_latency PRIVATE -O3 -march=native -DNDEBUG)
