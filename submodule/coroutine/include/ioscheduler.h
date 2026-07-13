#include "scheduler.h"
#include "fiber.h"
#include "timer.h"

#include <atomic>
#include <shared_mutex>

namespace moczkrin
{
    class IOManager : public Scheduler, public TimerManager
    {
    public:
        enum Event
        {
            NONE = 0x000,
            READ = 0x001, // 数值上正好等于 EPOLLIN
            WRITE = 0x004 // 数值上正好等于 EPOLLOUT
        };

    private:
        struct FdContext
        {
            struct EventContext
            {
                Scheduler *scheduler = nullptr;
                std::function<void()> cb;
                std::shared_ptr<Fiber> fiber_ptr;
            };

            EventContext read;
            EventContext write;

            int fd = 0;

            // enum 表示 任务的具体事件类型。
            // 用 bit 分别表示不同的类型，所以同一fd可能有多种事件
            Event events = NONE;

            std::mutex mutex;

            EventContext &getEventContext(Event event)
            {
                assert(event == READ || event == WRITE);
                switch (event)
                {
                case READ:
                    return read;
                case WRITE:
                    return write;
                }
                throw std::invalid_argument("Unsupported event type");
            }

            void resetEventContext(EventContext &eventContext)
            {
                eventContext.cb = nullptr;
                eventContext.scheduler = nullptr;
                eventContext.fiber_ptr.reset();
            }

            void triggerEvent(Event event)
            {
                assert(events & event);
                events = (Event)(events & ~event);
                EventContext eventContext = getEventContext(event);
                if (eventContext.cb)
                {
                    eventContext.scheduler->scheduleLock(eventContext.cb);
                }
                else if (eventContext.fiber_ptr)
                {
                    eventContext.scheduler->scheduleLock(eventContext.fiber_ptr);
                }
                resetEventContext(eventContext);
            }
        };

    public:
        IOManager(size_t threads = 1, bool use_caller = true, const std::string &name = "IOManager");
        ~IOManager();

        /**
         * 暴露四种外部调用
         */
        // add one event at a time
        int addEvent(int fd, Event event, std::function<void()> cb = nullptr);
        // delete event
        bool delEvent(int fd, Event event);
        // delete the event and trigger its callback
        bool cancelEvent(int fd, Event event);
        // delete all events and trigger its callback
        bool cancelAll(int fd);

        static IOManager *GetThis();

    protected:
        void tickle() override;

        bool stopping() override;

        void idle() override;

        void onTimerInsertedAtFront() override;

        void contextResize(size_t size);

        /**
         * 成员变量全部私有
         */
    private:
        int m_epfd = 0;
        // 读写管道
        int m_pip[2] = {0};

        // 原子变量计数
        std::atomic<uint64_t> m_pendingEventsCount = {0};

        // 多线程访问管理集合
        std::shared_mutex m_mutex;

        // FdContexts 管理集合
        std::vector<FdContext *> m_fdContexts;
    };
};
