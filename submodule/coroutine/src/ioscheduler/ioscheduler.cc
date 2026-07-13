#include <unistd.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <cstring>

#include "ioscheduler.h"

#include <iomanip> 

namespace moczkrin

{

    IOManager::IOManager(size_t threads , bool use_caller, const std::string &name ):
    Scheduler(threads, use_caller, name), TimerManager() 
    {
        // create epoll fd
        m_epfd = epoll_create(5000);
        assert(m_epfd > 0);

        // create pipe
        int rt = pipe(m_pip);
        assert(!rt);

        // add read event to epoll
        epoll_event event;
        event.events  = EPOLLIN | EPOLLET; // Edge Triggered
        event.data.fd = m_pip[0];

        // non-blocked
        rt = fcntl(m_pip[0], F_SETFL, O_NONBLOCK);
        assert(!rt);

        rt = epoll_ctl(m_epfd, EPOLL_CTL_ADD, m_pip[0], &event);
        assert(!rt);

        // contextResize(32);
        m_fdContexts.resize(32);

        for (size_t i = 0; i < m_fdContexts.size(); ++i) 
        {
            if (m_fdContexts[i]==nullptr) 
            {
                m_fdContexts[i] = new FdContext();
                m_fdContexts[i]->fd = i;
            }
        }

        // Scheduler::start();
        start();
    }
    
    IOManager::~IOManager() {
        stop();
        close(m_epfd);
        close(m_pip[0]);
        close(m_pip[1]);

        for (size_t i = 0; i < m_fdContexts.size(); ++i) 
        {
            if (m_fdContexts[i]) 
            {
                delete m_fdContexts[i];
            }
        }
    }


    int IOManager::addEvent(int fd, Event event, std::function<void()> cb)
    {
        if (fd < 0 || (event != READ && event != WRITE))
        {
            errno = EINVAL;
            std::cerr << "addEvent invalid fd or event: " << strerror(errno) << std::endl;
            return -1;
        }

        FdContext *fd_ptr;
        std::shared_lock<std::shared_mutex> read_lock(m_mutex);

        if (m_fdContexts.size() > fd)
        {
            fd_ptr = m_fdContexts[fd];
            read_lock.unlock();
        }
        else
        {
            read_lock.unlock();
            std::unique_lock<std::shared_mutex> write_lock(m_mutex);
            m_fdContexts.resize(fd * 2);
            for (size_t i = 0; i < fd * 2; i++)
            {
                if (m_fdContexts[i] == nullptr)
                {
                    m_fdContexts[i] = new FdContext();
                    m_fdContexts[i]->fd = i;
                }
            }

            fd_ptr = m_fdContexts[fd];
            // write_lock.unlock();
        }

        std::unique_lock<std::mutex> write_lock(fd_ptr->mutex);

        if (fd_ptr->events & event)
        {
            // 添加进来的事务和原先重复
            return -1;
        }

        const Event old_events = fd_ptr->events;
        const Event new_events = (Event)(old_events | event);

        epoll_event epevent{};
        // 第一次注册 fd 使用 ADD，已有事件时追加注册使用 MOD。
        int op           = old_events == NONE ? EPOLL_CTL_ADD : EPOLL_CTL_MOD;
        epevent.events   = static_cast<Event>(EPOLLET | (EPOLL_EVENTS)new_events);
        epevent.data.ptr = fd_ptr;

        // register and check
        // if return greater than 0, means fail.
        if (epoll_ctl(m_epfd, op, fd, &epevent))
        {
            std::cerr << "addEvent::epoll_ctl failed: " << strerror(errno) << std::endl;
            return -1;
        }

        fd_ptr->events = new_events;

        m_pendingEventsCount++;

        FdContext::EventContext &event_ctx = fd_ptr->getEventContext(event);
        // assert(!event_ctx.scheduler && !event_ctx.fiber && !event_ctx.cb);
        event_ctx.scheduler = Scheduler::GetThis();
        if (cb)
        {
            event_ctx.cb.swap(cb);
        }
        else
        {
            event_ctx.fiber_ptr = Fiber::GetThis();
            assert(event_ctx.fiber_ptr->getState() == Fiber::RUNNING);
        }
        return 0;
    }

    bool IOManager::delEvent(int fd, Event event)
    {
        FdContext *fd_ptr = nullptr;
        std::shared_lock<std::shared_mutex> read_lock(m_mutex);
        if (m_fdContexts.size() > fd)
        {
            fd_ptr = m_fdContexts[fd];
            read_lock.unlock();
        }
        else
        {
            read_lock.unlock();
            return false;
        }


        std::unique_lock<std::mutex> lock(fd_ptr->mutex);

        
        bool exist = fd_ptr->events & event;
        if (!exist)
        {
            return false;
        }

        fd_ptr->events = (Event)(fd_ptr->events & ~event);

        epoll_event epevent;
        int op              = fd_ptr->events ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;
        epevent.events      = static_cast<Event>(EPOLLET | static_cast<EPOLL_EVENTS>(fd_ptr->events));
        epevent.data.ptr    = fd_ptr;

        if (epoll_ctl(m_epfd, op, fd, &epevent))
        {
            std::cerr << "delEvent::epoll_ctl failed: " << strerror(errno) << std::endl;
            return -1;
        }

        m_pendingEventsCount.fetch_sub(1);

        FdContext::EventContext& event_ctx = fd_ptr->getEventContext(event);
        fd_ptr->resetEventContext(event_ctx);
        return true;
    }
    
    bool IOManager::cancelEvent(int fd, Event event)
    {
        std::shared_lock<std::shared_mutex> read_lock(m_mutex);

        assert(m_fdContexts.size() >= fd);
        
        FdContext* fd_ptr = nullptr;
        fd_ptr = m_fdContexts[fd];
        read_lock.unlock();

        std::unique_lock<std::mutex> lock(fd_ptr->mutex);
        if(fd_ptr == nullptr)
        {
            std::cerr << "cancelEvent delete fd: "  << fd << " failed: " << strerror(errno) << std::endl;
            return -1;
        }

        if(!(fd_ptr->events & event))
        {
            // 对应 fd 不存在 event 事件
            std::cerr << "cancelEvent delete fd: "  << fd << " event not exists: " << strerror(errno) << std::endl;
            return -2;
        }
        
        // 用户标记中删除 triggerEvent 真实执行
        Event newEvent = (Event) (fd_ptr->events & ~event);

        // 内核标记中删除
        epoll_event epevent;
        int op              = newEvent ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;
        epevent.events      =  static_cast<Event>(EPOLLET | static_cast<EPOLL_EVENTS>(newEvent));
        epevent.data.ptr    = fd_ptr;
        if(epoll_ctl(m_epfd, op, fd, &epevent))
        {
            std::cerr << "cancelEvent::epoll_ctl failed: " << strerror(errno) << std::endl; 
            return -1;
        }

        FdContext::EventContext& eventContext = fd_ptr->getEventContext(event);
        
        m_pendingEventsCount --;

        fd_ptr->triggerEvent(event);
        return true;
    }
            // delete all events and trigger its callback
    bool IOManager::cancelAll(int fd)
    {
        std::shared_lock<std::shared_mutex> read_lock(m_mutex);

        assert(m_fdContexts.size() >= fd);
        
        FdContext* fd_ptr = nullptr;
        fd_ptr = m_fdContexts[fd];
        read_lock.unlock();

        cancelEvent(fd, READ);
        cancelEvent(fd, WRITE);
        
        assert(fd_ptr->events == 0);
        return true;
    }

    IOManager *IOManager::GetThis()
    {
        return dynamic_cast<IOManager*>(Scheduler::GetThis());
    }

    //     protected:
    void IOManager::tickle() 
    {
        if(!hasIdleThreads()) 
        {
            return;
        }
        int rt = write(m_pip[1], "T", 1);
        assert(rt == 1);    
    }

    bool IOManager::stopping()
    {
        uint64_t timeout = getNextTimer();
        // no timers left and no pending events left with the Scheduler::stopping()
        return timeout == ~0ull && m_pendingEventsCount == 0 && Scheduler::stopping();
    }

    void IOManager::idle()
    {
        static const uint64_t MAX_EVNETS = 256;
        std::unique_ptr<epoll_event[]> events(new epoll_event[MAX_EVNETS]);
        
        while (true) 
        {
            if(stopping()) 
            {
                break;
            }

            int rt = 0;
            while(true)
            {
                static const uint64_t MAX_TIMEOUT = 5000;
                uint64_t next_timeout = getNextTimer();

                next_timeout = std::min(next_timeout, MAX_TIMEOUT);

                rt = epoll_wait(m_epfd, events.get(), MAX_EVNETS, (int)next_timeout);

                if(rt < 0 && errno == EINTR) 
                {
                    continue;
                } 
                else 
                {
                    break;
                }
            }

            std::vector<std::function<void()>> cbs;
            listExpiredCb(cbs);
            if(!cbs.empty()) 
            {
                for(const auto& cb : cbs) 
                {
                    scheduleLock(cb);
                }
                cbs.clear();
            }

              // collect all events ready
            for (int i = 0; i < rt; ++i) 
            {
                epoll_event& event = events[i];
            
                // tickle event
                if (event.data.fd == m_pip[0]) 
                {
                    uint8_t dummy[256];
                    // edge triggered -> exhaust
                    while (read(m_pip[0], dummy, sizeof(dummy)) > 0);
                    continue;
                }

                // other events
                FdContext *fd_ctx = (FdContext *)event.data.ptr;
                std::lock_guard<std::mutex> lock(fd_ctx->mutex);

                // convert EPOLLERR or EPOLLHUP to -> read or write event
                if (event.events & (EPOLLERR | EPOLLHUP)) 
                {
                    event.events |= (EPOLLIN | EPOLLOUT) & fd_ctx->events;
                }

                // events happening during this turn of epoll_wait
                int real_events = NONE;
                if (event.events & EPOLLIN) 
                {
                    real_events |= READ;
                }

                if (event.events & EPOLLOUT) 
                {
                    real_events |= WRITE;
                }

                if ((fd_ctx->events & real_events) == NONE) 
                {
                    continue;
                }

                // delete the events that have already happened
                int left_events = (fd_ctx->events & ~real_events);
                int op          = left_events ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;
                event.events    = EPOLLET | left_events;
                
                int rt2 = epoll_ctl(m_epfd, op, fd_ctx->fd, &event);
                if (rt2) 
                {
                    std::cerr << "idle::epoll_ctl failed: " << strerror(errno) << std::endl; 
                    continue;
                }   

                // schedule callback and update fdcontext and event context
                if (real_events & READ) 
                {
                    fd_ctx->triggerEvent(READ);
                    --m_pendingEventsCount;
                }
                if (real_events & WRITE) 
                {
                    fd_ctx->triggerEvent(WRITE);
                    --m_pendingEventsCount;
                }
            }  // end for 
            
            // test print out
            std::cout << "Fiber" << std::endl;
            // idle 线程一定需要在 while 循环里手动 yield，否则 idle 线程将在Fiber的MainFunc()中释放自己的栈
            Fiber::GetThis()->yield();
        }   // end while

    }

    void IOManager::onTimerInsertedAtFront() 
    {
        tickle();
    }
}
