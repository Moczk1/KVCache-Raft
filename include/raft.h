#include <iostream>
#include <cstdio>
#include <mutex>
#include <vector>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include <algorithm>
#include <random>
#include <chrono>
#include <cstdarg>
#include <ctime>
#include <iomanip>
#include "config.h"
#include "raftRPC.pb.h"
#include "raftRPC.grpc.pb.h"
#include "grpcpp/grpcpp.h"

namespace moczkrin
{

    class RaftService : public raftRpcProctoc::raftRpc::Service
    {
    public:
        grpc::Status AppendEntries(::grpc::ServerContext *context, const ::raftRpcProctoc::AppendEntriesArgs *request, ::raftRpcProctoc::AppendEntriesReply *response) override;
        grpc::Status InstallSnapshot(::grpc::ServerContext *context, const ::raftRpcProctoc::InstallSnapshotRequest *request, ::raftRpcProctoc::InstallSnapshotResponse *response) override;
        grpc::Status RequestVote(::grpc::ServerContext *context, const ::raftRpcProctoc::RequestVoteArgs *request, ::raftRpcProctoc::RequestVoteReply *response) override;

    private:
        enum VoteState
        ///////////////投票状态
        {
            Killed = 0,
            Voted = 1,
            Expire = 2,
            Normal = 3
        };
        std::shared_mutex m_mutex;

    public:
        VoteState m_voteState;
        int m_id = 0;
        std::string m_ip = "";
        std::string m_port = "";
        
        int m_currentTerm =0;
        // vote and state
        enum Status
        {
            Follower,
            Candidate,
            Leader
        };
        Status m_status = Follower;

        int m_votedFor;
        // vote and state

        //  persisted logs' term and index
        int m_lastSnapshotIncludeIndex = 0;
        int m_lastSnapshotIncludeTerm = 0;

        //  log collections
        std::vector<raftRpcProctoc::LogEntry> m_logs; //// 日志条目数组，包含了状态机要执行的指令集，以及收到领导时的任期号

        // log 同步用
        std::vector<int> m_nextIndex; // 这两个状态的下标1开始，因为通常commitIndex和lastApplied从0开始，应该是一个无效的index，因此下标从1开始
        std::vector<int> m_matchIndex;

        int m_commitIndex = -1;

        // time association
        std::chrono::system_clock::time_point m_lastResetElectionTime;

        // 心跳超时，用于leader
        std::chrono::system_clock::time_point m_lastResetHearBeatTime;

        // RaftNode calls peers' service
        std::vector<std::unique_ptr<raftRpcProctoc::raftRpc::Stub>> m_peers;
        std::vector<std::pair<std::string, std::string>> m_peers_addr;

        // RaftNode's listening interface
        std::unique_ptr<grpc::Server> m_serverInterface;

    public:
        void leaderHearBeatTicker();
        void doHeartBeat();
        bool sendAppendEntries(int serIdx, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                               std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply, std::shared_ptr<int> appendNums);

        void electionTimeOutTicker();
        void doElection();
        bool sendRequestVote(int peer_idx, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                             std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum);

        bool containsNewLog(int index, int term);
        void listening()
        {
            if (!m_serverInterface)
            {
                std::cerr << "gRPC server has not been started" << std::endl;
                return;
            }
            m_serverInterface->Wait();
        };

        RaftService() {};
        void init(std::string ip, std::string port, std::vector<std::pair<std::string, std::string>> peers);

        // bool addPeer(std::string ip, std::string port);

    private:
        std::chrono::milliseconds getRandomizedElectionTimeout()
        {
            std::random_device rd;
            std::mt19937 rng(rd());
            std::uniform_int_distribution<int> dist(minRandomizedElectionTime, maxRandomizedElectionTime);

            return std::chrono::milliseconds(dist(rng));
        }
    };

}
