#include <algorithm>
#include <stdexcept>
#include <string>
#include <thread>

#include "raft.h"
#include "config.h"

namespace moczkrin
{

    grpc::Status RaftService::AppendEntries(::grpc::ServerContext *context,
                                            const ::raftRpcProctoc::AppendEntriesArgs *request,
                                            ::raftRpcProctoc::AppendEntriesReply *response)
    {
        response->set_term(m_currentTerm);
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
            ;
        }
        else
        {
            m_votedFor = request->candidateid();
            m_lastResetElectionTime = std::chrono::high_resolution_clock::now(); // 认为必须要在投出票的时候才重置定时器，
            response->set_term(m_currentTerm);
            response->set_votestate(Normal);
            response->set_votegranted(true);
        }

        return grpc::Status::OK;
    }

    bool RaftService::sendRequestVote(int peer_idx, raftRpcProctoc::RequestVoteArgs *args,
                                      raftRpcProctoc::RequestVoteReply *reply, int *votedNum)
    {
        auto start = std::chrono::high_resolution_clock::now();
        grpc::ClientContext context;
        grpc::Status status = m_peers[peer_idx]->RequestVote(&context, *args, reply);

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
        if (*votedNum > m_peers.size() / 2 + 1)
        { // 成为 lead
            *votedNum = 0;
            if (m_status == Leader)
            {
                std::cout << __LINE__ << ":: leader node 申请再次成为 leader" << std::endl;
                return true;
            }

            m_status = Leader;
            std::cout << __LINE__ << ":: other state node 申请成为 leader" << std::endl;

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

    void RaftService::doHeartBeat()
    {
    }

    void RaftService::doElection()
    {
        std::unique_lock<std::shared_mutex> lock(m_mutex);
        if (m_status == Leader)
        {
            std::cout << __LINE__ << ":: leader node 申请再次成为 leader" << std::endl;
        }

        if (m_status != Leader)
        {
            std::cout << __LINE__ << "::\t id:" << m_id << "选举定时器到期且不是leader, 开始申请成为 leader" << std::endl;

            m_status = Candidate;
            m_currentTerm += 1;
            m_votedFor = m_id; // 自己给自己投票

            // persist();

            int votedNum = 1;

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

                std::thread t(&RaftService::sendRequestVote, this, i, requestVoteArgs.get(), requestVoteReply.get(),
                              &votedNum); // 创建新线程并执行b函数，并传递参数
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
                continue;
            }
            doElection();
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

    void RaftService::init(std::string ip, std::string port)
    {
        m_ip = ip;
        m_port = port;
        m_voteState = Normal;
        assert(m_serverInterface == nullptr);

        grpc::ServerBuilder builder;
        builder.AddListeningPort(m_ip + ":" + m_port, grpc::InsecureServerCredentials());

        builder.RegisterService(this);

        m_serverInterface = builder.BuildAndStart();
        if (!m_serverInterface)
        {
            throw std::runtime_error("failed to start gRPC server on " + m_ip + ":" + m_port);
        }
    }

    bool RaftService::addPeer(std::string ip, std::string port)
    {
        // std::string ip_port_ = ip + ":" + port;
        // if (ip.compare(m_ip) == 0 && port.compare(m_port) == 0)
        // {
        //     std::cout << "添加服务器地址为本地地址" << std::endl;
        //     return false;
        // }

        // if (m_peers.find(ip_port_) != m_peers.end())
        // {
        //     return true;
        // }

        // std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(ip_port_, grpc::InsecureChannelCredentials());
        // std::unique_ptr<raftRpcProctoc::raftRpc::Stub> stub = raftRpcProctoc::raftRpc::NewStub(channel);
        // m_peers.insert({ip_port_, std::move(stub)});

        return true;
    }

    void RaftService::leaderHearBeatTicker()
    {
    }

    void RaftService::electionTimeOutTicker()
    {
    }

    void RaftService::doElection()
    {
    }

}; // RaftService
