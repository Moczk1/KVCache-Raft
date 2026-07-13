#include "thread.h"
#include <sys/syscall.h>
#include <iostream>
#include <unistd.h>



namespace moczkrin
{

    const bool thread_debug_option = true;

    static thread_local Thread *t_thread = nullptr;
    static thread_local std::string t_thread_name = "UNKNOWN";

    pid_t Thread::GetThreadId()
    {
        return syscall(SYS_gettid);
    }

    Thread *Thread::GetThis()
    {
        return t_thread;
    }

    void Thread::SetName(const std::string &name)
    {
        if (t_thread)
        {
            t_thread->m_name = name;
        }
        t_thread_name = name;
    }

    const std::string &Thread::GetName()
    {
        return t_thread_name;
    }

    
    Thread::Thread(std::function<void()> cb, const std::string &name) : m_cb(cb), m_name(name)
    {
        if (thread_debug_option)
            std::cout << "Construction function called!" << std::endl;
    
        // std::thread 直接接收静态成员函数和参数
        // 注意：由于 std::thread 可能会抛出异常，这里可以使用 try-catch 捕获
        try
        {
            m_thread = std::thread(&Thread::run, this);
        }
        catch (const std::exception &e)
        {
            std::cerr << "std::thread create fail, name=" << name << ", error: " << e.what() << std::endl;
            throw std::logic_error("thread create error");
        }

        // 等待线程函数完成初始化
        m_semaphore.wait();
    }

    // 析构函数重构
    Thread::~Thread()
    {
        // 如果线程依然有效（没有被 join 或 detach），则必须调用 detach
        if (m_thread.joinable())
        {
            m_thread.detach();
        }
    }

    // join 重构
    void Thread::join()
    {
        if (m_thread.joinable())
        {
            try
            {
                if(thread_debug_option)
                    std::cout << "Thread join() called!" << std::endl;
                m_thread.join();
            }
            catch (const std::exception &e)
            {
                std::cerr << "std::thread join failed, name = " << m_name << ", error: " << e.what() << std::endl;
                throw std::logic_error("thread join error");
            }
        }
    }

    // 线程运行函数重构
    void Thread::run(Thread *thread)
    {
        t_thread        = thread;
        t_thread_name   = thread->m_name;
        
        thread->m_id    = GetThreadId();

        // Linux 下依然可以使用 pthread_setname_np 命名
        // 现代 C++ 提供了 native_handle() 拿到底层的 pthread_t
        pthread_setname_np(pthread_self(), thread->m_name.substr(0, 15).c_str());

        std::function<void()> cb;
        cb.swap(thread->m_cb);

        // 初始化完成，唤醒构造函数
        thread->m_semaphore.signal();

        if (cb)
        {
            cb();
        }
    }

}