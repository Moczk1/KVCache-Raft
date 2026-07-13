#pragma once

#include <mutex>
#include <condition_variable>
#include <functional>
#include <string>
#include <thread>
#include <iostream>

namespace moczkrin
{

    /**
     * control thread creation process.
     * stick to create a Thread instance then run it.
     */
    class Semaphore
    {
    private:
        std::mutex mtx;
        std::condition_variable cv;
        int count;

    public:
        explicit Semaphore(int count_ = 0) : count(count_) {}

        void wait()
        {
            std::unique_lock<std::mutex> lock(mtx);
            while (count == 0)
            {
                cv.wait(lock);
            }
            count--;
        }

        void signal()
        {
            std::unique_lock<std::mutex> lock(mtx);
            count++;
            cv.notify_one();
        }
    };

    /**
     * Creations on the heap are non-movable.
     */
    class Thread
    {
    public:
        // constructor and destructor
        Thread(std::function<void()> cb, const std::string &name);
        ~Thread();
        Thread(const Thread &other)
        {
            std::cout << "construction copy function called" << std::endl;
            m_id = other.getId();
        }

        /**
         * ordinary member functions
         */
        pid_t getId() const { return m_id; }
        const std::string &getName() const { return m_name; }
        void join();

    public:
        /**
         *  get and set variables in thread exclusive space.
         *  -----------
         *  static thread_local Thread *t_thread = nullptr;
         *  static thread_local std::string t_thread_name = "UNKNOWN";
         *  ---------------
         */
        static pid_t GetThreadId();
        static Thread *GetThis();
        static const std::string &GetName();
        static void SetName(const std::string &name);

    private:
        // Thread entry. We will run m_cb func in this function inside.
        static void run(Thread *thread);

    private:
        // system allocates
        pid_t m_id = -1;

        // C++ thread instance
        std::thread m_thread;

        // real need to be executed function
        std::function<void()> m_cb;

        // Thread instance's name
        std::string m_name;

        Semaphore m_semaphore;
    };

}
