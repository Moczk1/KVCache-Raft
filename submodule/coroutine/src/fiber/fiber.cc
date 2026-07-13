#include "fiber.h"

static bool debug = true;

namespace moczkrin
{

    // 当前线程上的协程控制信息
    // 正在运行的协程
    static thread_local Fiber *t_fiber = nullptr;
    // 主协程
    static thread_local std::shared_ptr<Fiber> t_thread_fiber = nullptr;
    // 调度协程
    static thread_local Fiber *t_scheduler_fiber = nullptr;

    // 协程计数器
    static std::atomic<uint64_t> s_fiber_id = {0};
    // 协程id
    static std::atomic<uint64_t> s_fiber_count = {0};

    void Fiber::SetThis(Fiber *f)
    {
        t_fiber = f;
    }

    /**
     * @brief For first time, create new Fiber instance, set three ptr to the same instance, 
     *        and set state to RUNNING,
     * @return std::shared_ptr<Fiber> ： new create fiber or the inRunningFiber instance.
     */
    std::shared_ptr<Fiber> Fiber::GetThis()
    {
        if(debug)
        {
            // std::cout << "s_fiber_id " << s_fiber_id << std::endl;
        }

        if (t_fiber)
        {
            return t_fiber->shared_from_this();
        }

        std::shared_ptr<Fiber> main_fiber(new Fiber());
        t_thread_fiber = main_fiber;
        t_scheduler_fiber = main_fiber.get(); // 除非主动设置 主协程默认为调度协程

        assert(t_fiber == main_fiber.get());
        return t_fiber->shared_from_this();
    }

    void Fiber::SetSchedulerFiber(Fiber *f)
    {
        t_scheduler_fiber = f;
    }

    uint64_t Fiber::GetFiberId()
    {
        if (t_fiber)
        {
            return t_fiber->getId();
        }
        return (uint64_t)-1;
    }

    /**
     * Empty constructor
     * Default settings for the RUNNING runtime state and context settings.
     * 
     * 空构造
     * 默认设置 RUNNING 运行状态，并获得上下文保存到 m_ctx，
     * 由 GetThis() 调用
     * 新调用的函数栈如未设置初 Fiber 对象则默认设置。 
     */
    Fiber::Fiber()
    {
        SetThis(this);
        m_state = RUNNING;

        if (getcontext(&m_ctx))
        {
            std::cerr << "Fiber() failed\n";
            pthread_exit(NULL);
        }

        m_id = s_fiber_id.fetch_add(1);
        s_fiber_count.fetch_add(1);
        if (debug)
            std::cout << "Fiber(): main id = " << m_id << std::endl;
    }

    /**
     * @brief 外部构造函数
     * @param cb 函数指针
     * @param stacksize 堆上分配的栈大小
     * @param run_in_scehduler bool: 调度的过程是否使用调度协程
     */
    Fiber::Fiber(std::function<void()> cb, size_t stacksize, bool run_in_scheduler) : m_cb(cb), m_runInScheduler(run_in_scheduler)
    {
        m_state = READY;

        // 分配协程栈空间
        m_stacksize = stacksize ? stacksize : 128000;
        m_stack = malloc(m_stacksize);

        if (getcontext(&m_ctx))
        {
            std::cerr << "Fiber(std::function<void()> cb, size_t stacksize, bool run_in_scheduler) failed\n";
            pthread_exit(NULL);
        }

        m_ctx.uc_link = nullptr;
        m_ctx.uc_stack.ss_sp = m_stack;
        m_ctx.uc_stack.ss_size = m_stacksize;
        makecontext(&m_ctx, &Fiber::MainFunc, 0);

        m_id = s_fiber_id.fetch_add(1);
        s_fiber_count.fetch_add(1);
        if (debug)
            std::cout << "Fiber(): child id = " << m_id << std::endl;
    }

    /**
     * descturctor 
     *        atomic variables s_fiber_count 自减；
     *        释放进程在堆上为协程分配的栈空间
     */
    Fiber::~Fiber()
    {
        s_fiber_count.fetch_sub(1);
        if (m_stack)
        {
            free(m_stack);
        }
        if (debug)
            std::cout << "~Fiber(): id = " << m_id << std::endl;
    }

    /**
     * 重置 Fiber 对象的 state 状态 和 cb 函数指针
     * 重新设置协程对象 Fiber 的上下文 
     * @param cb 传入的函数指针。
     */
    void Fiber::reset(std::function<void()> cb)
    {
        assert(m_stack != nullptr && m_state == TERM);

        m_state = READY;
        m_cb = cb;

        if (getcontext(&m_ctx))
        {
            std::cerr << "reset() failed\n";
            pthread_exit(NULL);
        }

        m_ctx.uc_link = nullptr;
        m_ctx.uc_stack.ss_sp = m_stack;
        m_ctx.uc_stack.ss_size = m_stacksize;
        makecontext(&m_ctx, &Fiber::MainFunc, 0);
    }

    /** 
     * Fiber 协程对象被调度去运行的入口
     */
    void Fiber::resume()
    {
        assert(m_state == READY);

        m_state = RUNNING;

        if (m_runInScheduler)
        {
            SetThis(this);
            if (swapcontext(&(t_scheduler_fiber->m_ctx), &m_ctx))
            {
                std::cerr << "resume() to t_scheduler_fiber failed\n";
                pthread_exit(NULL);
            }
        }
        else
        {
            SetThis(this);
            if (swapcontext(&(t_thread_fiber->m_ctx), &m_ctx))
            {
                std::cerr << "resume() to t_thread_fiber failed\n";
                pthread_exit(NULL);
            }
        }
    }
    /**
     * 交出CPU执行权限。
     */
    void Fiber::yield()
    {
        assert(m_state == RUNNING || m_state == TERM);

        if (m_state != TERM)
        {
            m_state = READY;
        }

        if (m_runInScheduler)
        {
            SetThis(t_scheduler_fiber);
            if (swapcontext(&m_ctx, &(t_scheduler_fiber->m_ctx)))
            {
                std::cerr << "yield() to to t_scheduler_fiber failed\n";
                pthread_exit(NULL);
            }
        }
        else
        {
            SetThis(t_thread_fiber.get());
            if (swapcontext(&m_ctx, &(t_thread_fiber->m_ctx)))
            {
                std::cerr << "yield() to t_thread_fiber failed\n";
                pthread_exit(NULL);
            }
        }
    }

    /**
     * 包装的 Fiber 运行的入口函数。
     * 如在创建过程中，调用了 yield() 则不会更改 Fiber state 的位置。
     * Fiber 对象的运行将不会退出。
     */
    void Fiber::MainFunc()
    {
        std::shared_ptr<Fiber> curr = GetThis();
        assert(curr != nullptr);

        curr->m_cb();
        curr->m_cb = nullptr;
        curr->m_state = TERM;

        // 运行完毕 -> 让出执行权
        auto raw_ptr = curr.get();
        curr.reset();
        raw_ptr->yield();
        assert(false); // 理论上不应该执行到这里
    }

}