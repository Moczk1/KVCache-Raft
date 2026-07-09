#include "clerk/clerk.h"

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace
{
using Clock = std::chrono::steady_clock;

struct Options
{
	std::string config = "test.conf";
	std::string workload = "mixed";
	int threads = 1;
	int durationSeconds = 30;
	int warmupSeconds = 5;
	int readRatio = 50;
	int keyCount = 10000;
	int valueSize = 128;
	std::uint64_t seed = 1;
	bool csvHeader = false;
};

struct Result
{
	std::uint64_t operations = 0;
	std::vector<std::uint64_t> latencyUs;
};

void usage(const char *program)
{
	std::cout
	    << "Usage: " << program << " [options]\n"
	    << "  --config PATH       cluster config file (default: test.conf)\n"
	    << "  --workload TYPE     read|write|mixed|append (default: mixed)\n"
	    << "  --threads N         concurrent clients (default: 1)\n"
	    << "  --duration SEC      measured duration (default: 30)\n"
	    << "  --warmup SEC        warm-up duration (default: 5)\n"
	    << "  --read-ratio N      read percentage for mixed workload\n"
	    << "  --key-count N       key-space size (default: 10000)\n"
	    << "  --value-size N      value bytes for writes (default: 128)\n"
	    << "  --seed N            random seed (default: 1)\n"
	    << "  --csv-header        print the CSV header before the result\n";
}

std::string requireValue(int &index, int argc, char **argv)
{
	if (++index >= argc)
	{
		throw std::runtime_error("missing value for " + std::string(argv[index - 1]));
	}
	return argv[index];
}

Options parseArgs(int argc, char **argv)
{
	Options options;
	for (int i = 1; i < argc; ++i)
	{
		const std::string arg = argv[i];
		if (arg == "--help" || arg == "-h")
		{
			usage(argv[0]);
			std::exit(EXIT_SUCCESS);
		}
		else if (arg == "--config")
			options.config = requireValue(i, argc, argv);
		else if (arg == "--workload")
			options.workload = requireValue(i, argc, argv);
		else if (arg == "--threads")
			options.threads = std::stoi(requireValue(i, argc, argv));
		else if (arg == "--duration")
			options.durationSeconds = std::stoi(requireValue(i, argc, argv));
		else if (arg == "--warmup")
			options.warmupSeconds = std::stoi(requireValue(i, argc, argv));
		else if (arg == "--read-ratio")
			options.readRatio = std::stoi(requireValue(i, argc, argv));
		else if (arg == "--key-count")
			options.keyCount = std::stoi(requireValue(i, argc, argv));
		else if (arg == "--value-size")
			options.valueSize = std::stoi(requireValue(i, argc, argv));
		else if (arg == "--seed")
			options.seed = std::stoull(requireValue(i, argc, argv));
		else if (arg == "--csv-header")
			options.csvHeader = true;
		else
			throw std::runtime_error("unknown option: " + arg);
	}

	if (options.threads <= 0 || options.durationSeconds <= 0 ||
	    options.warmupSeconds < 0 || options.keyCount <= 0 ||
	    options.valueSize < 0 || options.readRatio < 0 || options.readRatio > 100)
	{
		throw std::runtime_error("numeric option is outside its valid range");
	}
	if (options.workload != "read" && options.workload != "write" &&
	    options.workload != "mixed" && options.workload != "append")
	{
		throw std::runtime_error("workload must be read, write, mixed, or append");
	}
	return options;
}

bool shouldRead(const Options &options, std::mt19937_64 &random)
{
	if (options.workload == "read")
		return true;
	if (options.workload != "mixed")
		return false;
	return std::uniform_int_distribution<int>(0, 99)(random) < options.readRatio;
}

void execute(mraft::Clerk &client, const Options &options,
    std::mt19937_64 &random, const std::string &value)
{
	const auto keyIndex =
	    std::uniform_int_distribution<int>(0, options.keyCount - 1)(random);
	const std::string key = "benchmark-key-" + std::to_string(keyIndex);
	if (shouldRead(options, random))
		client.Get(key);
	else if (options.workload == "append")
		client.Append(key, value);
	else
		client.Put(key, value);
}

std::uint64_t percentile(
    const std::vector<std::uint64_t> &sorted, double quantile)
{
	if (sorted.empty())
		return 0;
	const auto index = static_cast<std::size_t>(
	    quantile * static_cast<double>(sorted.size() - 1));
	return sorted[index];
}

} // namespace

int main(int argc, char **argv)
{
	try
	{
		const Options options = parseArgs(argc, argv);
		std::vector<Result> results(options.threads);
		std::barrier ready(options.threads + 1);
		std::atomic<bool> measure{false};
		std::atomic<bool> stop{false};
		std::vector<std::thread> workers;
		std::vector<std::unique_ptr<mraft::Clerk>> clients;
		workers.reserve(options.threads);
		clients.reserve(options.threads);

		// Clerk::Uuid() uses the process-wide rand(), so construct clients
		// serially and keep one independent Clerk per worker.
		for (int workerId = 0; workerId < options.threads; ++workerId)
		{
			auto client = std::make_unique<mraft::Clerk>();
			client->Init(options.config);
			clients.push_back(std::move(client));
		}

		for (int workerId = 0; workerId < options.threads; ++workerId)
		{
			workers.emplace_back(
			    [&, workerId]
			    {
				    auto &client = *clients[workerId];
				    std::mt19937_64 random(options.seed + workerId);
				    const std::string value(options.valueSize, 'x');
				    auto &result = results[workerId];
				    ready.arrive_and_wait();

				    while (!stop.load(std::memory_order_relaxed))
				    {
					    const bool measuredOperation =
					        measure.load(std::memory_order_relaxed);
					    const auto begin = Clock::now();
					    execute(client, options, random, value);
					    const auto end = Clock::now();
					    if (measuredOperation &&
					        measure.load(std::memory_order_relaxed))
					    {
						    result.operations++;
						    result.latencyUs.push_back(
						        std::chrono::duration_cast<std::chrono::microseconds>(
						            end - begin)
						            .count());
					    }
				    }
			    });
		}

		ready.arrive_and_wait();
		std::this_thread::sleep_for(
		    std::chrono::seconds(options.warmupSeconds));
		const auto measuredStart = Clock::now();
		measure.store(true, std::memory_order_relaxed);
		std::this_thread::sleep_for(
		    std::chrono::seconds(options.durationSeconds));
		measure.store(false, std::memory_order_relaxed);
		const auto measuredEnd = Clock::now();
		stop.store(true, std::memory_order_relaxed);

		for (auto &worker : workers)
			worker.join();

		std::uint64_t operationCount = 0;
		std::vector<std::uint64_t> latencies;
		for (auto &result : results)
		{
			operationCount += result.operations;
			latencies.insert(latencies.end(), result.latencyUs.begin(),
			    result.latencyUs.end());
		}
		std::sort(latencies.begin(), latencies.end());

		const double seconds =
		    std::chrono::duration<double>(measuredEnd - measuredStart).count();
		const double throughput = operationCount / seconds;
		const int effectiveReadRatio =
		    options.workload == "read" ? 100
		    : options.workload == "mixed" ? options.readRatio
		                                  : 0;

		if (options.csvHeader)
		{
			std::cout << "workload,threads,read_ratio,key_count,value_size,"
			             "duration_s,operations,qps,p50_us,p90_us,p99_us,p999_us\n";
		}
		std::cout << options.workload << ',' << options.threads << ','
		          << effectiveReadRatio << ',' << options.keyCount << ','
		          << options.valueSize << ',' << std::fixed << std::setprecision(3)
		          << seconds << ',' << operationCount << ',' << throughput << ','
		          << percentile(latencies, 0.50) << ','
		          << percentile(latencies, 0.90) << ','
		          << percentile(latencies, 0.99) << ','
		          << percentile(latencies, 0.999) << '\n';
	}
	catch (const std::exception &error)
	{
		std::cerr << "benchmark: " << error.what() << '\n';
		return EXIT_FAILURE;
	}
}
