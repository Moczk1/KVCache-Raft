#include "scheduler.h"
#include "sys/syscall.h"
#include "hook.h"

static bool debug = false;

namespace moczkrin
{
    // 线程私有 只在主线程中持有
    static thread_local Scheduler *t_scheduler = nullptr;

    Scheduler *Scheduler::GetThis()
    {
        return t_scheduler;
    }

    void Scheduler::SetThis()
    {
        t_scheduler = this;
    }

    /**
     * @brief Construction Function
     * @param threads numbers of thread needed
     * @param use_caller whether use main thread as a work thread
     * @param name scheduler instance name
     */
    Scheduler::Scheduler(size_t threads, bool use_caller, const std::string &name) : m_useCaller(use_caller), m_name(name)
    {
        assert(threads > 0 );

        
        
        // 使用主线程当作工作线程
        if (use_caller)
        {
            
            assert(Scheduler::GetThis() == nullptr);
            // scheduler 构造过程中设置
            SetThis();
            Thread::SetName(m_name);
            
            threads--;
            // 创建主协程
            Fiber::GetThis();
            // 创建调度协程对于主线程的模型为   main-fiber <-> worker-fiber
            m_schedulerFiber.reset(new Fiber(std::bind(&Scheduler::run, this), 0, false)); // false -> 该调度协程退出后将返回主协程
            Fiber::SetSchedulerFiber(m_schedulerFiber.get());  // 主线程参与执行任务，设置额外的调度Fiber instance 到主线程的 Fiber：：t_scheduler_fiber
            
            m_rootThread = Thread::GetThreadId();
            m_threadIds.push_back(m_rootThread);
        }
            
        m_threadCount = threads;
        if (debug)
            std::cout << "Scheduler::Scheduler() success\n";
    }

    Scheduler::~Scheduler()
    {
        assert(stopping() == true);
        if (GetThis() == this)
        {
            t_scheduler = nullptr;
        }
        if (debug)
            std::cout << "Scheduler::~Scheduler() success\n";
    }

    void Scheduler::start()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        // 关闭过程中，不再执行初始化任务
        if (m_stopping)
        {
            std::cerr << "Scheduler is stopped" << std::endl;
            return;
        }

        // start 必须满足开始的线程池为空
        assert(m_threads.empty());

        // 线程池初始化和运行
        m_threads.resize(m_threadCount);
        for (size_t i = 0; i < m_threadCount; i++)
        {
            // 创建线程对象 
            m_threads[i].reset(new Thread(std::bind(&Scheduler::run, this), m_name + "_" + std::to_string(i)));
            // 保存线程id
            m_threadIds.push_back(m_threads[i]->getId());
        }

        if (debug)
            std::cout << "Scheduler::start() success\n";
    }

    /**
     * 为新创建的线程提供入口方法。
     */
    void Scheduler::run()
    {
        // SYS allocated 线程id
        int thread_id = Thread::GetThreadId();
        if (debug)
            std::cout << "Schedule::run() starts in thread: " << thread_id << std::endl;

        // set_hook_enable(true);

        SetThis();

        //运行在新创建的线程 -> 需要创建主协程
        if (thread_id != m_rootThread)
        {
            // 将新线程的线程内部变量保存的 三个 fiber 指针初始化为自己的 Fiber 对象
            // 保存上下文到 Fiber::m_ctx
            Fiber::GetThis();
        }

        // 设置 scheduler 空闲时的执行逻辑
        std::shared_ptr<Fiber> idle_fiber = std::make_shared<Fiber>(std::bind(&Scheduler::idle, this));
        ScheduleTask task;
        while (true)
        {
            task.reset();
            bool tickle_me = false;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                auto it = m_tasks.begin();
                // 1 遍历任务队列
                while (it != m_tasks.end())
                {
                    // 判断任务是否由本线程完成
                    if (it->thread != -1 && it->thread != thread_id)
                    {
                        it++;
                        tickle_me = true;  // 非本线程完成，设置唤醒函数变量
                        continue;
                    }

                    // 2 取出任务
                    assert(it->fiber || it->cb);
                    task = *it;
                    m_tasks.erase(it);      // 任务队列中删除
                    m_activeThreadCount.fetch_add(1);
                    break;
                }
                tickle_me = tickle_me || (it != m_tasks.end());
            }
            
            if (tickle_me)
            {
                tickle();
            }

            // 3 执行任务
            if (task.fiber)
            {
                {
                    std::lock_guard<std::mutex> lock(task.fiber->m_mutex);
                    if (task.fiber->getState() != Fiber::TERM)
                    {
                        task.fiber->resume();
                    }
                }
                m_activeThreadCount.fetch_sub(1);
                task.reset();
            }
            else if (task.cb)
            {
                std::shared_ptr<Fiber> cb_fiber = std::make_shared<Fiber>(task.cb);
                {
                    std::lock_guard<std::mutex> lock(cb_fiber->m_mutex);
                    cb_fiber->resume();
                }
                m_activeThreadCount.fetch_sub(1);
                task.reset();
            }
            // 4 无任务 -> 执行空闲协程
            else
            {
                // 系统关闭 -> idle协程将从死循环跳出并结束 -> 此时的idle协程状态为TERM -> 再次进入将跳出循环并退出run()
                if (idle_fiber->getState() == Fiber::TERM)
                {
                    if (debug)
                        std::cout << "Schedule::run() ends in thread: " << thread_id << std::endl;
                    break;
                }
                m_idleThreadCount.fetch_add(1);
                idle_fiber->resume();   // idle 自己
                m_idleThreadCount.fetch_sub(1);
            }
        }
        assert(m_stopping == true);     // 调度结束
    }


    /**
     * 主（外部）线程中主动调用 用于结束调度。
     */
    void Scheduler::stop()
    {
        if (debug)
            std::cout << "Schedule::stop() starts in thread: " << Thread::GetThreadId() << std::endl;

        if (stopping())
        {
            // 全部完成直接返回
            return;
        }
        // 还剩下任务未完成
        m_stopping = true;

        if (m_useCaller)
        {
            // 创建 scheduler instance 时在主线程设置了线程变量
            assert(GetThis() == this);
        }
        else
        {
            assert(GetThis() != this);
        }

        for (size_t i = 0; i < m_threadCount; i++)
        {
            tickle();
        }

        if (m_schedulerFiber)
        {
            tickle();
        }

        if (m_schedulerFiber)
        {
            m_schedulerFiber->resume();
            if (debug)
                std::cout << "m_schedulerFiber ends in thread:" << Thread::GetThreadId() << std::endl;
        }

        for (auto &i : m_threads)
        {
            i->join();
        }
        if (debug)
            std::cout << "Schedule::stop() ends in thread:" << Thread::GetThreadId() << std::endl;
    }

    void Scheduler::tickle()
    {
    }


    /**
     * @brief idle 线程的执行逻辑
     */
    void Scheduler::idle()
    {
        // 检查 scheduler 的运行状态；关闭过程中则跳出到 Fiber::MainFunc() 的退出逻辑
        while (!stopping())
        {
            if (debug)
            {
                std::cout << "Scheduler::idle(), sleeping in thread: " << Thread::GetThreadId() << std::endl;
            }
            sleep(1);
            Fiber::GetThis()->yield();  // 主动 yield() 防止跳出while循环，执行到 Fiber::MainFunc() 的退出逻辑
        }
    }

    /**
     * @return true - 执行 stop() 更改 m_stopping 为 true；任务全部执行完毕；执行任务线程清零；则返回 true
     */
    bool Scheduler::stopping()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_stopping && m_tasks.empty() && m_activeThreadCount == 0;
    }

}