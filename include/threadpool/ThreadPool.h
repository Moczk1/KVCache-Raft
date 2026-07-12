#pragma once
#include <algorithm>
#include <atomic>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <future>
#include <list>
#include <memory>
#include <mutex>
#include <print>
#include <queue>
#include <string>
#include <sys/types.h>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>
namespace moczkrin
{

// 任务模板
struct Task
{
	std::move_only_function<void()> function;

	int priority = 0;

	Task() = default;

	template <class F> Task(int p, F &&f) : priority(p), function(std::move(f))
	{
	}

	void operator()() { function(); }

	bool operator<(const Task &other) const noexcept
	{
		return priority < other.priority;
	}

	Task(Task &&) noexcept = default;
	Task &operator=(Task &&) noexcept = default;
};

struct Worker
{
	int id = 0;
	std::thread thread;
	std::atomic<bool> exited{false};
};

class ThreadPool
{
	ThreadPool(const ThreadPool &) = delete;
	ThreadPool(ThreadPool &&) = delete;
	ThreadPool &operator=(ThreadPool &) = delete;
	ThreadPool &operator=(ThreadPool &&) = delete;

  public:
	// 队列类型
	enum QueueType
	{
		LinkedBlockingQueue,
		SynchronousQueue,
		DelayedWorkQueue,
	};

	enum TimeUnit
	{
		minute,
		second,
		millisecond,
		microsecond,
		nanoseconde,
	};

	enum RejectStrategy
	{
		AbortPolicy,
		DiscardPolicy,
		DiscardOldestPolicy,
		CallerRunsPolicy,
	};

  private:
	bool Debug = true;
	// 线程池名称
	std::string m_name = "nullptr";

	RejectStrategy mRjectStrategy = AbortPolicy;

	// 主线程数
	const size_t mCorePoolSize = 0;
	// 临时线程数
	const size_t mMaxPoolSize = 0;

	// 持续时间长度
	size_t mKeepAliveTime;
	// 时间单位
	TimeUnit mTimeUnit;

	QueueType mQueueType;
	// 队列
	std::priority_queue<Task> mTaskQueue;
	// 队列容量上限
	size_t mQueueSizeLimit;

	// 线程存储结构
	// std::vector<std::thread> mThreads;
	std::list<std::unique_ptr<Worker>> mWorkers;
	std::mutex mWorkerMutex;

	std::condition_variable mEventVar;
	std::mutex mEventMutex;

	// 已创建的核心线程 + 非核心线程
	std::atomic<size_t> mCurrentPoolSize = 0;

	// 正在运行任务的线程数 mActiveCount <= mCurrentPoolSize
	std::atomic<size_t> mActiveCount = 0;

	bool mStopping = false;

  public:
	/**
	  无返回结果的任务
	   */
	template <class F, class... Args>
	void execute(int priority, F &&f, Args &&...args)
	{
		auto work = [function = std::forward<F>(f),
		                arguments = std::make_tuple(
		                    std::forward<Args>(args)...)]() mutable
		{
			std::apply(
			    [&function](auto &&...values)
			    {
				    std::invoke(std::move(function),
				        std::forward<decltype(values)>(values)...);
			    },
			    std::move(arguments));
		};

		Task task(priority, std::move(work));
		std::unique_lock<std::mutex> lock(mEventMutex);

		// 当前线程数小于核心线程数，直接创建核心线程运行
		if (mCurrentPoolSize < mCorePoolSize)
		{
			if (addWorker(std::move(task)))
			{
				// std::print("thread pool size = {}\n", pool_size());
				return;
			}
			// 如果创建失败（比如刚好处于停止状态），重新拿到任务
		}

		bool queue_has_space = mQueueSizeLimit != 0 ? true : false;

		if (queue_has_space && mTaskQueue.size() < mQueueSizeLimit)
		{
			mTaskQueue.emplace(std::move(task));
			mEventVar.notify_one();
			return;
		}
		// 3. 队列也满了，尝试创建非核心（临时）线程
		if (mCurrentPoolSize < mMaxPoolSize)
		{
			if (addWorker(std::move(task)))
			{
				return;
			}
		}

		/** rs 拒绝策略 */
		throw std::runtime_error("ThreadPool queue is full, task rejected.");

		// rs 拒绝策略
	}

	/** 有返回结果的任务提交 */
	template <class F, class... Args>
	auto submit(int priority, F &&f, Args &&...args)
	{

		using Result =
		    std::invoke_result_t<std::decay_t<F>, std::decay_t<Args>...>;

		auto callable = [function = std::forward<F>(f),
		                    arguments = std::make_tuple(std::forward<Args>(
		                        args)...)]() mutable -> Result
		{
			return std::apply(
			    [&function](auto &&...values) -> Result
			    {
				    return std::invoke(std::move(function),
				        std::forward<decltype(values)>(values)...);
			    },
			    std::move(arguments));
		};

		std::packaged_task<Result()> packaged(std::move(callable));
		std::future<Result> future = packaged.get_future();

		Task task(priority,
		    [packaged = std::move(packaged)]() mutable { packaged(); });

		std::unique_lock<std::mutex> lock(mEventMutex);

		bool queue_has_space = mQueueSizeLimit != 0 ? true : false;

		if (mCurrentPoolSize < mCorePoolSize)
		{
			if (addWorker(std::move(task)))
			{
				return future;
			}
		}

		/** core 已经满 加入队列中 */

		if (queue_has_space && mTaskQueue.size() < mQueueSizeLimit)
		{
			mTaskQueue.emplace(std::move(task));
			mEventVar.notify_one();
			return future;
		}

		// 3. 队列也满了，尝试创建非核心（临时）线程
		if (mCurrentPoolSize < mMaxPoolSize)
		{
			if (addWorker(std::move(task)))
			{
				return future;
			}
		}

		/** rs 拒绝策略 */
		throw std::runtime_error("ThreadPool queue is full, task rejected.");
	}

	// 传递结束标志给 threadpool 停止接受新任务.
	void shutdown();

	// 立即结束,返回未执行的任务,交由主线程自行处理
	std::vector<Task> shutdown_now();

	bool is_shutdown() const noexcept;

	bool is_terminating() const noexcept;

	bool is_terminated() const noexcept;

	inline std::size_t core_pool_size() const noexcept { return mCorePoolSize; }

	std::size_t maximum_pool_size() const noexcept { return mMaxPoolSize; }

	std::size_t pool_size() const noexcept { return mCurrentPoolSize; }

	std::size_t active_count() const noexcept { return mActiveCount; }

	std::uint64_t completed_task_count() const noexcept;

	/**
	public ThreadPoolExecutor(int corePoolSize,//线程池的核心线程数量
	                          int maximumPoolSize,//线程池的最大线程数
	                          long
	keepAliveTime,//当线程数大于核心线程数时，多余的空闲线程存活的最长时间
	                          TimeUnit unit,//时间单位
	                          BlockingQueue<Runnable>
	workQueue,//任务队列，用来储存等待执行任务的队列 ThreadFactory
	threadFactory,//线程工厂，用来创建线程，一般默认即可
	                          RejectedExecutionHandler handler/
	                           */
	ThreadPool(size_t corePoolSize, size_t maxPoolSize, long keepAliveTime,
	    TimeUnit unit, QueueType qtype = LinkedBlockingQueue, size_t qsize = INT_MAX - 1,
	    std::string name = "nullptr", RejectStrategy rs = AbortPolicy);

	void makequue(QueueType);
	~ThreadPool() { stop(); }

	// void enqueue(std::function<void()> task, int priority);

  private:
	std::chrono::nanoseconds getKeepAliveTime();

	// 内部创建线程的统一入口，返回是否创建成功
	bool addWorker(Task &&firstTask);

	void workerLoop(Worker *self, Task firstTask);

	void reapExitedWorkers();

	void stop();
};

} // namespace moczkrin