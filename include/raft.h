#pragma once
#include "raftRPC.pb.h"
#include "raftRPC.grpc.pb.h"
#include "raftRpcUtil.h"
#include "grpcpp/grpcpp.h"
#include <mutex>
#include <vector>
#include "ioscheduler.h"

namespace moczkrin
{

    ///////////////投票状态
    constexpr int Killed = 0;
    constexpr int Voted = 1;  // 本轮已经投过票了
    constexpr int Expire = 2; // 投票（消息、竞选者）过期
    constexpr int Normal = 3;

    class RaftService : public raftRpc::Service
    {
    private:
        std::mutex m_mutex;
        std::vector<std::shared_ptr<RaftRpcUtil>> m_peers;
        int m_id;
        int m_currentTerm;
        int m_votedFor;

        std::vector<LogEntry> m_logs;

        int m_commitIndex;
        int m_lastApplied; // 已经汇报给状态机（上层应用）的log 的index

        // 这两个状态是由服务器来维护，易失
        std::vector<int> m_nextIndex;
        std::vector<int> m_matchIndex; // 这两个状态的下标1开始，因为通常commitIndex和lastApplied从0开始，应该是一个无效的index，因此下标从1开始

        enum Status
        {
            Follower,
            Candidate,
            Leader
        };
        Status m_status;

        // 选举超时
        std::chrono::_V2::system_clock::time_point m_lastResetElectionTime;
        // 心跳超时，用于leader
        std::chrono::_V2::system_clock::time_point m_lastResetHearBeatTime;


        // // 2D中用于传入快照点
        // // 储存了快照中的最后一个日志的Index和Term
        // int m_lastSnapshotIncludeIndex;
        // int m_lastSnapshotIncludeTerm;

        std::unique_ptr<moczkrin::IOManager> m_ioManager = nullptr;
    

    public:

        void leaderHearBeatTicker() ;

        ::grpc::Status AppendEntries(::grpc::ServerContext *context,
                                     const ::AppendEntriesArgs *request,
                                     ::AppendEntriesReply *response) override;
        ::grpc::Status InstallSnapshot(::grpc::ServerContext *context,
                                       const ::InstallSnapshotRequest *request,
                                       ::InstallSnapshotResponse *response) override;

        grpc::Status RequestVote(::grpc::ServerContext *context,
                                 const RequestVoteArgs *request,
                                 RequestVoteReply *response) override;
        
        
        void init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me);

    }; // RaftService
}