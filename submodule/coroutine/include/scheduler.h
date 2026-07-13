#pragma once
#include "fiber.h"
#include "thread.h"
#include <mutex>
#include <vector>

namespace moczkrin
{
    /**
     * 多线程公用一个 scheduler 调度对象。
     * 可提供后续继承自行更改内部实现
     */
    class Scheduler
    {
    public:
        Scheduler(size_t threads = 1, bool use_caller = true,
                  const std::string &name = "Scheduler");
        virtual ~Scheduler();

        const std::string &getName() const { return m_name; }

    public:
        // 获取正在运行的调度器
        static Scheduler *GetThis();

    protected:
        // 设置正在运行的调度器
        void SetThis();

    public:
        /**
         * 添加任务到任务队列
         * @param fc template<function or Fiber >
         * @param thread 期望的执行函数逻辑的线程id
         *  
         */ 
        template <class FiberOrCb>
        void scheduleLock(FiberOrCb fc, int thread = -1)
        {
            bool need_tickle;
            {
                
                std::lock_guard<std::mutex> lock(m_mutex);
                // empty ->  all thread is idle -> need to be
                // waken up
                need_tickle = m_tasks.empty();

                ScheduleTask task(fc, thread);
                if (task.fiber || task.cb)
                {
                    m_tasks.push_back(task);
                }
            }
            // thread needs to be keep alive 
            if (need_tickle)
            {
                tickle();
            }
        }

        /**
         * 对外暴露的两个方法。
         */
        virtual void start();
        virtual void stop();

    protected:
        virtual void tickle();

        // 线程函数
        virtual void run();

        // 空闲协程函数：可自行处理 空闲状态下 scheduler 的执行逻辑
        virtual void idle();

        // 判断 scheduler 对象是否已经处于关闭的处理过程中。
        virtual bool stopping();

        // 判断调度器是否
        bool hasIdleThreads() const { return m_idleThreadCount > 0; }

    private:
        // 任务结构体
        struct ScheduleTask
        {
            std::shared_ptr<Fiber> fiber;
            std::function<void()> cb;
            int thread; // 指定任务需要运行的线程id

            ScheduleTask()
            {
                fiber = nullptr;
                cb = nullptr;
                thread = -1;
            }

            ScheduleTask(std::shared_ptr<Fiber> f, int thr)
            {
                fiber = f;
                thread = thr;
            }

            ScheduleTask(std::shared_ptr<Fiber> *f, int thr)
            {
                fiber.swap(*f);
                thread = thr;
            }

            ScheduleTask(std::function<void()> f, int thr)
            {
                cb = f;
                thread = thr;
            }

            ScheduleTask(std::function<void()> *f, int thr)
            {
                cb.swap(*f);
                thread = thr;
            }

            void reset()
            {
                fiber = nullptr;
                cb = nullptr;
                thread = -1;
            }
        };

    private:
        std::string m_name;
        // 互斥锁 -> 保护任务队列
        std::mutex m_mutex;
        // 线程池
        std::vector<std::shared_ptr<Thread>> m_threads;
        // 任务队列
        std::vector<ScheduleTask> m_tasks;
        // 存储工作线程的线程id
        std::vector<int> m_threadIds;
        // 需要额外创建的线程数
        size_t m_threadCount = 0;
        // 活跃线程数
        std::atomic<size_t> m_activeThreadCount = {0};
        // 空闲线程数
        std::atomic<size_t> m_idleThreadCount = {0};

        // 主线程是否用作工作线程
        bool m_useCaller;
        // 如果是 -> 需要额外创建调度协程
        std::shared_ptr<Fiber> m_schedulerFiber;
        // 如果是 -> 记录主线程的线程id
        int m_rootThread = -1;
        // 是否正在关闭
        bool m_stopping = false;
    };

}
