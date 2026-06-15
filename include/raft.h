#pragma once
#include <iostream>
#include <cstdio>
#include <mutex>
#include <vector>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>


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
        int m_id = -1;
        int m_term = -1;
        std::string m_ip;
        std::string m_port;

        // RaftNode calls peers' service
        std::unordered_map<std::string, std::unique_ptr<raftRpcProctoc::raftRpc::Stub>> m_peers;

        // RaftNode's listening interface
        std::unique_ptr<grpc::Server> m_serverInterface;


    public:
        void leaderHearBeatTicker();
        void electionTimeOutTicker();
        void doElection();
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
        void init(std::string ip, std::string port);
        bool addPeer(std::string ip, std::string port);
    };

}
