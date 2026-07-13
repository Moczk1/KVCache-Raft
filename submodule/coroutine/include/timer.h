#include <memory>
#include <vector>
#include <set>
#include <shared_mutex>
#include <assert.h>
#include <functional>
#include <mutex>

namespace moczkrin
{
    class TimerManager;
    class Timer : public std::enable_shared_from_this<Timer>
    {
        friend class TimerManager;

    private:
        bool m_recurring = false;
        uint64_t m_timespan;
        std::chrono::time_point<std::chrono::system_clock> m_nextTimePoint;
        std::function<void()> m_cb;
        TimerManager *m_manager;

        struct Comparator
        {
            bool operator()(const std::shared_ptr<Timer> &t1, const std::shared_ptr<Timer> &t2) const
            {
                assert(t1 != t2);
                return t1->m_nextTimePoint < t2->m_nextTimePoint;
            }
        };

    public:
        bool cancel();
        bool refresh();
        bool reset(uint64_t timespan, bool from_now);

    private:
        Timer(uint64_t ms, std::function<void()> cb, bool recurring, TimerManager *manager);
    };

    class TimerManager
    {
        friend class Timer;

    public:
        TimerManager();
        virtual ~TimerManager();
        std::shared_ptr<Timer> addTimer(uint64_t ms, std::function<void()> cb, bool recurring = false);

        std::shared_ptr<Timer> addConditionTimer(uint64_t ms, std::function<void()> cb, std::weak_ptr<void> weak_cond, bool recurring = false);

        // 拿到堆中最近的超时时间
        uint64_t getNextTimer();

        // 取出所有超时定时器的回调函数
        void listExpiredCb(std::vector<std::function<void()>> &cbs);

        // 堆中是否有timer
        bool hasTimer() ;

    protected:
        // 当一个最早的timer加入到堆中 -> 调用该函数
        virtual void onTimerInsertedAtFront() {};

        // 添加timer
        void addTimer(std::shared_ptr<Timer> timer);

    private:
        // 当系统时间改变时 -> 调用该函数
        bool detectClockRollover();

    private:
        std::shared_mutex m_mutex;

        // 时间堆
        std::set<std::shared_ptr<Timer>, Timer::Comparator> m_timers;
        // 在下次getNextTime()执行前 onTimerInsertedAtFront()是否已经被触发了 -> 在此过程中 onTimerInsertedAtFront()只执行一次
        bool m_tickled = false;
        // 上次检查系统时间是否回退的绝对时间
        std::chrono::time_point<std::chrono::system_clock> m_previouseTime;
    };
}