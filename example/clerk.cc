#include "clerk/clerk.h"

#include <chrono>
#include <print>
#include <string>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;

struct ThreadStat
{
    std::chrono::milliseconds elapsed{0};
};

void worker(int threadId, int count, ThreadStat& stat)
{
    mraft::Clerk client;
    client.Init("test.conf");

    std::string key = "key_" + std::to_string(threadId);

    auto start = Clock::now();

    for (int i = 0; i < count; ++i)
    {
        client.Put(key, std::to_string(i));

        auto value = client.Get(key);

        // 防止编译器优化
        if (value.empty() && i == -1)
        {
            std::print("{}\n", value);
        }
    }

    auto end = Clock::now();

    stat.elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
}

int main()
{
    constexpr int threadNum = 1;
    constexpr int countPerThread = 5000;

    std::vector<std::jthread> threads;
    std::vector<ThreadStat> stats(threadNum);

    auto benchStart = Clock::now();

    for (int i = 0; i < threadNum; ++i)
    {
        threads.emplace_back(worker, i, countPerThread, std::ref(stats[i]));
    }

    // std::jthread 会自动 join()

    threads.clear();

    auto benchEnd = Clock::now();

    double totalSeconds =
        std::chrono::duration<double>(benchEnd - benchStart).count();

    const int totalOps = threadNum * countPerThread * 2;

    double throughput = totalOps / totalSeconds;

    double avgLatencyMs =
        totalSeconds * 1000.0 / totalOps;

    std::chrono::milliseconds longest{0};

    for (const auto& s : stats)
    {
        if (s.elapsed > longest)
            longest = s.elapsed;
    }

    std::print("\n========== Raft Benchmark ==========\n");
    std::print("Threads              : {}\n", threadNum);
    std::print("Requests/thread      : {}\n", countPerThread);
    std::print("Total operations     : {}\n", totalOps);
    std::print("Elapsed              : {:.3f} s\n", totalSeconds);
    std::print("Throughput           : {:.2f} ops/s\n", throughput);
    std::print("Average latency      : {:.4f} ms/op\n", avgLatencyMs);
    std::print("Longest thread time  : {} ms\n", longest.count());
    std::print("====================================\n");

    return 0;
}