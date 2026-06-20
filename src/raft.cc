#include <algorithm>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <linux/sched.h>
#include <sched.h>
#include <format>
#include <print>

#include "raft.h"
#include "config.h"

namespace moczkrin
{

    grpc::Status RaftService::AppendEntries(::grpc::ServerContext *context,
                                            const ::raftRpcProctoc::AppendEntriesArgs *request,
                                            ::raftRpcProctoc::AppendEntriesReply *response)
    {
        // response->set_term(m_currentTerm);
        std::unique_lock<std::shared_mutex> lock(m_mutex);
        if (request->term() < m_currentTerm)
        {
            response->set_success(false);
            response->set_term(m_currentTerm);
            response->set_updatenextindex(-100);
            std::print("{}:{}::leader{}->nodeid{}; lterm{}->nterm{}..REFUSE:: lterm < nterm\n",
                       __FUNCTION__, __LINE__,
                       request->leaderid(), m_id,
                       request->term(), m_currentTerm);
            // std::print(info);
            return grpc::Status::OK;
        }

        // persist();

        if (request->term() > m_currentTerm)
        {
            std::print(
                "{}:{}::leader{}->nodeid{}; lterm{}->nterm{}..ACCEPT:: lterm > nterm\n",
                __FUNCTION__, __LINE__,
                request->leaderid(), m_id,
                request->term(), m_currentTerm);
            m_status = Follower;
            m_currentTerm = request->term();
            m_votedFor = -1;
        }

        assert(request->term() == m_currentTerm);

        m_status = Follower;
        m_lastResetElectionTime = std::chrono::high_resolution_clock::now();

        int lastLogIndex = -1;
        if (request->prevlogindex() > [this, &lastLogIndex]() -> int
            {
                lastLogIndex = m_logs.empty() ? m_lastSnapshotIncludeIndex : m_logs[m_logs.size() - 1].logindex();
                return lastLogIndex;
            }())
        {
            response->set_success(true);
            response->set_term(m_currentTerm);
            response->set_updatenextindex(lastLogIndex + 1); // 运行在临界区域上的 raft集群，若 leader 还未受到满足commit数量的回复便挂掉，新的leader怎么处理这个日志。
            std::print(
                "{}:{}::leader{}->nodeid{}; lterm{}->nterm{}; lIdx:{}->nIdx:{}...ACCEPT:: term equal & lIdx > nIdx",
                __FUNCTION__, __LINE__,
                request->leaderid(), m_id,
                request->term(), m_currentTerm,
                request->prevlogindex(), lastLogIndex);
            return grpc::Status::OK;
        }
        else if (request->prevlogindex() < m_lastSnapshotIncludeIndex)
        {
            response->set_success(false);
            response->set_term(m_currentTerm);
            response->set_updatenextindex(m_lastSnapshotIncludeIndex + 1);
            std::print(
                "{}:{}::leader{}->nodeid{}; lterm{}->nterm{}; lIdx:{}->nIdx:{}...REFUSE:: lIdx < nIdx",
                __FUNCTION__, __LINE__,
                request->leaderid(), m_id,
                request->term(), m_currentTerm,
                request->prevlogindex(), lastLogIndex);
            return grpc::Status::OK;
        }

        // leader index 比自己日志的 index 小，需要进行一致性同步。
        // 先判断消息传递的最新的 index：term 是否和自己的 index：term 匹配
        // check true -> 完全增量同步
        bool check = [this, request, lastLogIndex]() -> bool
        {
            assert(request->prevlogindex() >= m_lastSnapshotIncludeIndex && request->prevlogindex() <= lastLogIndex);

            int lastLogTerm = -1;
            if (request->prevlogindex() == m_lastSnapshotIncludeIndex)
            {
                lastLogTerm = m_lastSnapshotIncludeTerm;
            }
            else
            {
                lastLogTerm = m_logs[m_logs.size() - 1].logterm();
            }
            return lastLogTerm == request->prevlogterm();
        }();

        // 匹配 ->  相同的 term 相同的 index：term
        if (check == true)
        {
            for (int i = 0; i < request->entries_size(); i++)
            {
                auto log = request->entries(i);
                // 消息日志的 index 大于 自己的 ：index 直接添加
                if (log.logindex() > lastLogIndex)
                {
                    m_logs.push_back(log);
                }
                else
                {
                    // 消息日志的 index 小于等于 自己的 ：index 需要进行匹配过程
                    // 系统要求 相同的 term 相同的LeaderCommit index； log 的 command 也必须一致。
                    assert(log.logindex() > m_lastSnapshotIncludeIndex && log.logindex() <= lastLogIndex);
                    if (m_logs[log.logindex() - m_lastSnapshotIncludeIndex - 1].logterm() == log.logterm() &&
                        m_logs[log.logindex() - m_lastSnapshotIncludeIndex - 1].command() != log.command())
                    {
                        std::print(
                            "{}:{}::leader{}->nodeid{},; lterm:{}->nterm:{}; 存在日志内容不匹配。系统故障！",
                            __FUNCTION__, __LINE__,
                            request->leaderid(), m_id,
                            request->term(), m_currentTerm,
                            request->prevlogindex(), lastLogIndex);
                    }

                    // 系统状态正常，进行正常更新；
                    if (m_logs[log.logindex() - m_lastSnapshotIncludeIndex - 1].logterm() != log.logterm())
                    {
                        m_logs[log.logindex() - m_lastSnapshotIncludeIndex - 1] = log;
                    }
                }
            }

            // 同步完之后，lead的日志大小应该和本地的日志大小一致。
            // 但是我们进行了持久化操作，所以比较的就应该是 log 表的 index 标识。
            assert(lastLogIndex >= request->prevlogindex() + request->entries_size());
            // 处理完日志的复制保存，后续就是提交状态的同步。
            if (request->leadercommit() > m_commitIndex)
            {
                m_commitIndex = std::min({request->leadercommit(), lastLogIndex});
            }

            // leader 会一次 rpc 中发送所有消息，需要保证 commit index 小于 log index
            assert(lastLogIndex >= m_commitIndex);

            response->set_success(true);
            response->set_term(m_currentTerm);

            std::print(
                "{}:{}::leadid:{}->nodeod:{}; 接受log, 当前日志的index:{}, 当前日志的commit index:{}\n",
                __FUNCTION__, __LINE__,
                request->leaderid(), m_id,
                lastLogIndex, m_commitIndex);
            return grpc::Status::OK;
        }
        else
        {
            // check false -> 出现 index 相同，但 term 不相同的情况，需要回退本地日志 table
            response->set_updatenextindex(m_lastSnapshotIncludeIndex);

            int conflict_term = m_logs[request->prevlogindex() - m_lastSnapshotIncludeIndex - 1].logterm();
            for (int index = request->prevlogindex(); index >= m_lastSnapshotIncludeIndex; index--)
            {
                if (m_logs[index - m_lastSnapshotIncludeIndex - 1].logterm() != conflict_term)
                {
                    response->set_updatenextindex(index + 1);
                    break;
                }
            }
            response->set_success(true);
            response->set_term(m_currentTerm);
        }

        return grpc::Status::OK;
    }

    grpc::Status RaftService::InstallSnapshot(::grpc::ServerContext *context,
                                              const ::raftRpcProctoc::InstallSnapshotRequest *request,
                                              ::raftRpcProctoc::InstallSnapshotResponse *response)
    {
        response->set_term(m_currentTerm);
        return grpc::Status::OK;
    }

    grpc::Status RaftService::RequestVote(::grpc::ServerContext *context,
                                          const ::raftRpcProctoc::RequestVoteArgs *request,
                                          ::raftRpcProctoc::RequestVoteReply *response)
    {
        // response->set_term(m_term);
        std::unique_lock<std::shared_mutex> lock(m_mutex);
        // 接受到的 term 值小于自己的 term -> 拒绝
        if (request->term() < m_currentTerm)
        {
            response->set_term(m_currentTerm);
            response->set_votestate(Expire);
            response->set_votegranted(false);
            return grpc::Status::OK;
            ;
        }
        //  这里还需要判断 日志 index 后续改进
        if (request->term() > m_currentTerm) // 对方的时间在自己之后，接受；跟随
        {
            m_status = Follower;
            m_currentTerm = request->term();
            m_votedFor = -1; // 时间强制条件转变为 Follower 状态，本轮 requestVote 并未自主投票，投票记录标记为-1；
        }

        if (m_currentTerm != request->term())
        {
            std::cout << "[info]:line" << __LINE__ << "::前面校验过reqest.Term==rf.currentTerm，这里却不等" << std::endl;
        }
        int lastLogTerm = m_logs.empty() ? m_lastSnapshotIncludeTerm : m_logs[m_logs.size() - 1].logterm();
        if (!containsNewLog(request->lastlogindex(), request->lastlogterm()))
        {
            if (request->lastlogterm() < lastLogTerm) // 传递过来的日志逻辑时间在自己的过去
            {
            }
            else
            {
                // lastLogTerm == m_currentTerm && request->lastLogIndex < lastLogIndex;
            }
            // 填写拒绝消息
            response->set_term(m_currentTerm);
            response->set_votestate(Voted);
            response->set_votegranted(false);
            return grpc::Status::OK;
            ; // 结束  返回
        }
        // 脑裂下的同一节点受到不同节点的 lead 请求，第二个被拒绝。
        if (m_votedFor != -1 && request->candidateid() != m_votedFor) // 接受了其他人的lead请求 且 并非原先的 lead
        {                                                             // 填写拒绝消息
            response->set_term(m_currentTerm);
            response->set_votestate(Voted);
            response->set_votegranted(false);
            return grpc::Status::OK;
        }
        else
        {
            m_votedFor = request->candidateid();
            m_lastResetElectionTime = std::chrono::high_resolution_clock::now(); // 认为必须要在投出票的时候才重置定时器，
            std::cout << __FUNCTION__ << ":" << __LINE__ << "::\t 投票成功功能新选举时间" << std::endl;
            response->set_term(m_currentTerm);
            response->set_votestate(Normal);
            response->set_votegranted(true);
        }

        return grpc::Status::OK;
    }

    bool RaftService::sendRequestVote(int peer_idx, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                                      std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum)
    {
        std::print(
            "{}:{}\t\t::nodeid:{} send vote request to nodeid:{}\n", __FUNCTION__, __LINE__,
            m_id, peer_idx + 1);
        auto start = std::chrono::high_resolution_clock::now();
        grpc::ClientContext context;
        grpc::Status status = m_peers[peer_idx]->RequestVote(&context, *args, reply.get());

        if (!status.ok())
        {
            // 网络状态异常
            return false;
        }

        std::unique_lock<std::shared_mutex> lock(m_mutex);
        if (reply->term() > m_currentTerm)
        {
            m_currentTerm = reply->term();
            m_status = Follower;
            m_votedFor = -1;
            // persist();

            return true;
        }
        else if (reply->term() < m_currentTerm)
        {
            return true;
        }

        assert(reply->term() == m_currentTerm);

        if (!reply->votegranted())
        {
            return true;
        }

        *votedNum = *votedNum + 1;
        if (*votedNum >= m_peers.size() / 2 + 1)
        { // 成为 lead
            assert(m_status == Candidate);
            std::cout << "m_id: " << m_id << "成为 leader!" << std::endl;
            *votedNum = 0;
            if (m_status == Leader)
            {
                std::cout << __LINE__ << ":: leader node 申请再次成为 leader" << std::endl;
                return true;
            }

            m_status = Leader;
            std::print("{}:{}::{}state node 申请成为 leader", __FUNCTION__, __LINE__, m_id);

            int lastLogIndex = m_logs.empty() ? m_lastSnapshotIncludeIndex : m_logs[m_logs.size() - 1].logindex();
            for (int i = 0; i < m_nextIndex.size(); i++)
            {
                m_nextIndex[i] = lastLogIndex + 1;
                m_matchIndex[i] = 0;
            }
            std::thread t(&RaftService::doHeartBeat, this);
            t.detach();

            // persist();
        }
        return true;
    }

    void RaftService::doElection()
    {
        std::unique_lock<std::shared_mutex> lock(m_mutex);
        if (m_status == Leader)
        {
            // std::cout << __LINE__ << "" << std::endl;
            std::print("{}:{}::\t\t leader node 申请再次成为 leader.\n", __FUNCTION__, __LINE__);
        }

        if (m_status != Leader)
        {
            std::cout << __LINE__ << "::\t id:" << m_id << "选举定时器到期且不是leader, 开始申请成为 leader" << std::endl;

            m_status = Candidate;
            m_currentTerm += 1;
            m_votedFor = m_id; // 自己给自己投票

            // persist();

            std::shared_ptr<int> votedNum = std::make_shared<int>(1);

            m_lastResetElectionTime = std::chrono::high_resolution_clock::now();

            for (int i = 0; i < m_peers.size(); i++)
            {
                int lastLogIndex = m_logs.empty() ? m_lastSnapshotIncludeIndex : m_logs[m_logs.size() - 1].logindex();
                int lastLogTerm = m_logs.empty() ? m_lastSnapshotIncludeTerm : m_logs[m_logs.size() - 1].logterm();
                std::shared_ptr<raftRpcProctoc::RequestVoteArgs> requestVoteArgs =
                    std::make_shared<raftRpcProctoc::RequestVoteArgs>();

                requestVoteArgs->set_term(m_currentTerm);
                requestVoteArgs->set_candidateid(m_id);
                requestVoteArgs->set_lastlogindex(lastLogIndex);
                requestVoteArgs->set_lastlogterm(lastLogTerm);

                auto requestVoteReply = std::make_shared<raftRpcProctoc::RequestVoteReply>();

                std::thread t(&RaftService::sendRequestVote, this, i, requestVoteArgs, requestVoteReply,
                              votedNum); // 创建新线程并执行b函数，并传递参数
                t.detach();
            }
        }
    }

    void RaftService::electionTimeOutTicker()
    {
        while (true)
        {
            while (m_status == Leader)
                usleep(HeartBeatTimeout);

            std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
            std::chrono::system_clock::time_point wakeTime{};
            {
                std::unique_lock<std::shared_mutex> lock(m_mutex);
                wakeTime = std::chrono::high_resolution_clock::now();
                suitableSleepTime = getRandomizedElectionTimeout() + m_lastResetElectionTime - wakeTime;
            }

            if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1)
            {
                // 获取当前时间点
                auto start = std::chrono::steady_clock::now();

                usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());
                // std::this_thread::sleep_for(suitableSleepTime);

                // 获取函数运行结束后的时间点
                auto end = std::chrono::steady_clock::now();

                // 计算时间差并输出结果（单位为毫秒）
                std::chrono::duration<double, std::milli> duration = end - start;

                // 使用ANSI控制序列将输出颜色修改为紫色
                std::cout << "\033[1;35m electionTimeOutTicker();函数设置睡眠时间为: "
                          << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() << " 毫秒\033[0m"
                          << std::endl;
                std::cout << "\033[1;35m electionTimeOutTicker();函数实际睡眠时间为: " << duration.count() << " 毫秒\033[0m"
                          << std::endl;
            }

            if (std::chrono::duration<double, std::milli>(m_lastResetElectionTime - wakeTime).count() > 0)
            {
                // 说明睡眠的这段时间有重置定时器，那么就没有超时，再次睡眠
                // std::cout << __FUNCTION__ << ":" << __LINE__ << "::\t" << "" << std::endl;
                std::print(
                    "{}:{}::\t\t更新过m_lastResetElectionTime,不执行 doElection() 等待下一次 heartBeat()\n",
                    __FUNCTION__, __LINE__);
                continue;
            }
            doElection();
            if (m_status != Leader)
            {
                std::print("{}:{}::\t\t没成为 leader!term:{}\n", __FUNCTION__, __LINE__, m_currentTerm);
            }
            if (m_status == Leader)
            {
                std::print("{}:{}::\t\t成为 leader!term:{}\n", __FUNCTION__, __LINE__, m_currentTerm);
            }
        }
    }

    void RaftService::leaderHearBeatTicker()
    {
        while (true)
        {
            while (m_status != Leader)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(HeartBeatTimeout));
            }
            static std::atomic<int32_t> atomicCount = 0;

            std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
            std::chrono::system_clock::time_point wakeTime{};
            {
                std::lock_guard<std::shared_mutex> lock(m_mutex);
                wakeTime = std::chrono::high_resolution_clock::now();
                suitableSleepTime = std::chrono::milliseconds(HeartBeatTimeout) + m_lastResetHearBeatTime - wakeTime;
            }

            if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1)
            {
                std::cout << atomicCount << "\033[1;35m leaderHearBeatTicker();函数设置睡眠时间为: "
                          << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() << " 毫秒\033[0m"
                          << std::endl;
                // 获取当前时间点
                auto start = std::chrono::steady_clock::now();

                usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());
                // std::this_thread::sleep_for(suitableSleepTime);

                // 获取函数运行结束后的时间点
                auto end = std::chrono::steady_clock::now();

                // 计算时间差并输出结果（单位为毫秒）
                std::chrono::duration<double, std::milli> duration = end - start;

                // 使用ANSI控制序列将输出颜色修改为紫色
                std::cout << atomicCount << "\033[1;35m leaderHearBeatTicker();函数实际睡眠时间为: " << duration.count()
                          << " 毫秒\033[0m" << std::endl;
                ++atomicCount;
            }

            if (std::chrono::duration<double, std::milli>(m_lastResetHearBeatTime - wakeTime).count() > 0)
            {
                // 睡眠的这段时间有重置定时器，没有超时，再次睡眠
                continue;
            }
            // DPrintf("[func-Raft::doHeartBeat()-Leader: {}] Leader的心跳定时器触发了\n", m_me);
            doHeartBeat();
        }
    }
    bool RaftService::sendAppendEntries(int serIdx, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                                        std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply, std::shared_ptr<int> appendNums)
    {

        // grpc::Status RaftService::AppendEntries(::grpc::ServerContext *context,
        //                                 const ::raftRpcProctoc::AppendEntriesArgs *request,
        //                                 ::raftRpcProctoc::AppendEntriesReply *response)

        std::print("{}:{}::\t\tleaderid:{} 向节点{}发送AE\n",
                   __FUNCTION__, __LINE__, m_id, serIdx);
        grpc::ClientContext context;
        grpc::Status status = m_peers[serIdx]->AppendEntries(&context, *args, reply.get());

        if (!status.ok())
        {
            std::print("{}:{}::\t\tleaderid:{} 向节点{}发送AE rpc 失败！\n",
                       __FUNCTION__, __LINE__, m_id, serIdx);
            return false;
        }
        std::print(
            "{}:{}::\t\tleaderid:{} 向节点{}发送AE rpc 成功！\n", __FUNCTION__, __LINE__,
            m_id, serIdx);

        // if()

        std::unique_lock<std::shared_mutex> lock(m_mutex);

        if (reply->term() > m_currentTerm)
        {
            std::print(
                "{}:{}::\t\tleaderid:{} 向节点{}发送AE 对方term:{} 大于自己的term:{} 自身状态转变为 Follower！\n",
                __FUNCTION__, __LINE__, m_id, serIdx,
                reply->term(), m_currentTerm);
            m_status = Follower;
            m_votedFor = -1;
            m_currentTerm = reply->term();
            return true;
        }

        if (reply->term() < m_currentTerm)
        {
            std::print(
                "{}:{}::\t\tleaderid:{} 向节点{}发送AE 对方term:{} 小于自己的term:{}！不进行任何操作！\n",
                __FUNCTION__, __LINE__, m_id, serIdx,
                reply->term(), m_currentTerm);
            return true;
        }
        assert(reply->term() == m_currentTerm);

        if (m_status != Leader)
        {
            std::print("{}:{}::\t\t接受到rpc reply 后自身状态发生变化。不再是Leader！不进行后续处理。\n",
                       __FUNCTION__, __LINE__);
            return true;
        }

        if (!reply->success())
        {
            // 日志不成功匹配
            if (reply->updatenextindex() != -100)
            {
                std::print("{}:{}\t\t::leaderid:{} 向节点{}发送AE。发生不匹配回缩nextIndex[]:{}！\n",
                           __FUNCTION__, __LINE__,
                           m_id, serIdx, serIdx, reply->updatenextindex());
                m_nextIndex[serIdx] = reply->updatenextindex();
            }
        }
        else
        {
            *appendNums += 1;

            if (DEBUG)
                std::print("{}:{}\t\t::节点:{}返回true，当前appendNums{}\n", __FUNCTION__, __LINE__,
                           serIdx, *appendNums);

            m_matchIndex[serIdx] = std::max(m_matchIndex[serIdx], args->prevlogindex() + args->entries_size());
            m_nextIndex[serIdx] = m_matchIndex[serIdx] + 1;

            int lastLogIndex = m_logs.empty() ? m_lastSnapshotIncludeIndex : m_logs[m_logs.size() - 1].logindex();

            assert(m_nextIndex[serIdx] <= lastLogIndex + 1);

            if (*appendNums == 1 + m_peers.size() / 2)
            {
                if (args->entries_size() > 0)
                {
                    assert(args->entries(args->entries_size() - 1).logterm() == m_currentTerm);
                    std::print("{}:{}\t\t::leader:{}成功提交，更新leader的m_commitIndex\n",
                               __FUNCTION__, __LINE__, m_id);
                    m_commitIndex = std::max(m_commitIndex, args->prevlogindex() + args->entries_size());
                }
                assert(m_commitIndex <= lastLogIndex);
            }
        }
        return true;
    }

    void RaftService::doHeartBeat()
    {
        std::unique_lock<std::shared_mutex> lock(m_mutex);
        std::string info;

        if (m_status == Leader)
        {
            m_lastResetHearBeatTime = std::chrono::high_resolution_clock::now();
            // m_lastResetElectionTime = std::chrono::high_resolution_clock::now();

            std::print("{}:{}\t\t::leader:{}_term:{}拿到了 mutex 并进行 heartBeat()发送 AE!\n",
                       __FUNCTION__, __LINE__, m_id, m_currentTerm);
            auto appendNum = std::make_shared<int>(1);
            for (int i = 0; i < m_peers.size(); i++)
            {
                std::print("{}:{}\t\t::leader:{}_term:{} heartBeat() 向{}发送 AE!\n",
                           __FUNCTION__, __LINE__, m_id, m_currentTerm, i);
                assert(m_nextIndex[i] >= 1);
                if (m_nextIndex[i] <= m_lastSnapshotIncludeIndex)
                {
                    continue;
                }
                int prevLogIndex = -1;
                int prevLogTerm = -1;
                if (m_nextIndex[i] == m_lastSnapshotIncludeIndex + 1)
                {
                    prevLogIndex = m_lastSnapshotIncludeIndex;
                    prevLogTerm = m_lastSnapshotIncludeTerm;
                    // std::print("{}:{}::nodeid:{} 没有发送 AE!\n", __FUNCTION__, __LINE__, m_id);
                    // std::print("m_lastSnapshotIncludeIndex:{}\n", m_lastSnapshotIncludeIndex);
                    // std::print("m_lastSnapshotIncludeTerm:{}\n", m_lastSnapshotIncludeTerm);
                    std::print("m_commitIndex:{}\n", m_commitIndex);
                    // for (int i = 0; i < m_nextIndex.size(); i++)
                    //     std::print("m_nextIndex{}:{}\n", i, m_nextIndex[i]);

                    // for (int i = 0; i < m_matchIndex.size(); i++)
                    //     std::print("m_matchIndex{}:{}\n", i, m_matchIndex[i]);

                    prevLogIndex = m_nextIndex[i] - 1;
                    // prevLogTerm = m_logs[prevLogIndex - m_lastSnapshotIncludeIndex - 1].logterm();
                    prevLogTerm = m_lastSnapshotIncludeTerm;
                    std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> appendEntriesArgs = std::make_shared<raftRpcProctoc::AppendEntriesArgs>();
                    appendEntriesArgs->set_term(m_currentTerm);
                    appendEntriesArgs->set_leaderid(m_id);
                    appendEntriesArgs->set_prevlogindex(prevLogIndex);
                    appendEntriesArgs->set_prevlogterm(prevLogTerm);
                    appendEntriesArgs->clear_entries();
                    appendEntriesArgs->set_leadercommit(m_commitIndex);
                    // prevLogIndex != m_lastSnapIncludeIndex
                    for (int j = [this, prevLogIndex]() -> int
                         {
                             return prevLogIndex - m_lastSnapshotIncludeIndex - 1;
                         }();
                         j < m_logs.size(); j++)
                    {
                        raftRpcProctoc::LogEntry *sendEntryPtr = appendEntriesArgs->add_entries();
                        *sendEntryPtr = m_logs[j];
                    }

                    int lastLogIndex = m_logs.empty() ? m_lastSnapshotIncludeIndex : m_logs[m_logs.size() - 1].logindex();

                    assert(appendEntriesArgs->prevlogindex() + appendEntriesArgs->entries_size() == lastLogIndex);

                    const std::shared_ptr<raftRpcProctoc::AppendEntriesReply> appendEntriesReply =
                        std::make_shared<raftRpcProctoc::AppendEntriesReply>();
                    // appendEntriesReply->set_appstate()
                    std::thread t(&RaftService::sendAppendEntries, this, i, appendEntriesArgs, appendEntriesReply, appendNum);
                    t.detach();
                }
                else
                {
                    prevLogIndex = m_nextIndex[i] - 1;
                    prevLogTerm = m_logs[prevLogIndex - m_lastSnapshotIncludeIndex - 1].logterm();
                    std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> appendEntriesArgs = std::make_shared<raftRpcProctoc::AppendEntriesArgs>();
                    appendEntriesArgs->set_term(m_currentTerm);
                    appendEntriesArgs->set_leaderid(m_id);
                    appendEntriesArgs->set_prevlogindex(prevLogIndex);
                    appendEntriesArgs->set_prevlogterm(prevLogTerm);
                    appendEntriesArgs->clear_entries();
                    appendEntriesArgs->set_leadercommit(m_commitIndex);
                    // prevLogIndex != m_lastSnapIncludeIndex
                    for (int j = [this, prevLogIndex]() -> int
                         {
                             return prevLogIndex - m_lastSnapshotIncludeIndex - 1;
                         }();
                         j < m_logs.size(); j++)
                    {
                        raftRpcProctoc::LogEntry *sendEntryPtr = appendEntriesArgs->add_entries();
                        *sendEntryPtr = m_logs[j];
                    }

                    int lastLogIndex = m_logs.empty() ? m_lastSnapshotIncludeIndex : m_logs[m_logs.size() - 1].logindex();

                    assert(appendEntriesArgs->prevlogindex() + appendEntriesArgs->entries_size() == lastLogIndex);

                    const std::shared_ptr<raftRpcProctoc::AppendEntriesReply> appendEntriesReply =
                        std::make_shared<raftRpcProctoc::AppendEntriesReply>();
                    // appendEntriesReply->set_appstate()
                    std::thread t(&RaftService::sendAppendEntries, this, i, appendEntriesArgs, appendEntriesReply, appendNum);
                    t.detach();
                }
                // m_lastResetElectionTime = std::chrono::high_resolution_clock::now();
            }
        }
    }

    bool RaftService::containsNewLog(int index, int term)
    {
        int lastIndex = -1;
        int lastTerm = -1;
        if (m_logs.empty())
        {
            lastIndex = m_lastSnapshotIncludeIndex;
            lastTerm = m_lastSnapshotIncludeTerm;
        }
        else
        {
            lastIndex = m_logs[m_logs.size() - 1].logindex();
            lastTerm = m_logs[m_logs.size() - 1].logterm();
        }
        return term > lastTerm || (term == lastTerm && index >= lastIndex);
    }

    void RaftService::init(std::string ip, std::string port, std::vector<std::pair<std::string, std::string>> peers)
    {
        m_ip = ip;
        m_port = port;
        m_voteState = Normal;
        assert(m_serverInterface == nullptr);
        m_lastResetElectionTime = std::chrono::high_resolution_clock::now();
        m_lastResetHearBeatTime = m_lastResetElectionTime;

        grpc::ServerBuilder builder;
        builder.AddListeningPort(m_ip + ":" + m_port, grpc::InsecureServerCredentials());

        builder.RegisterService(this);

        m_serverInterface = builder.BuildAndStart();
        if (!m_serverInterface)
        {
            throw std::runtime_error("failed to start gRPC server on " + m_ip + ":" + m_port);
        }

        int count = 0;

        m_currentTerm = 0;
        m_status = Follower;
        m_commitIndex = 0;

        for (const auto &[ip, port] : peers)
        {
            count++;
            if (ip == m_ip && port == m_port)
            {
                this->m_id = count;
                std::cout << m_id << std::endl;
                continue;
            }
            std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(ip + ":" + port, grpc::InsecureChannelCredentials());
            std::unique_ptr<raftRpcProctoc::raftRpc::Stub> stub = raftRpcProctoc::raftRpc::NewStub(channel);
            assert(stub != nullptr);
            m_peers.push_back(std::move(stub));
            m_peers_addr.push_back({ip, port});

            m_matchIndex.push_back(0);
            m_nextIndex.push_back(0);
        }

        std::thread t([this]()
                      { this->electionTimeOutTicker(); });
        t.detach();

        std::thread t2([this]()
                       { this->leaderHearBeatTicker(); });
        t2.detach();

        assert(m_ip != "" && m_port != "");
        this->listening();
    }

}; // RaftService
