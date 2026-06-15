#include "raft.h"
#include <algorithm>
#include <stdexcept>
#include <string>

namespace moczkrin
{
    
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
        std::string ip_port_ = ip + ":" + port;
        if (ip.compare(m_ip) == 0 && port.compare(m_port) == 0)
        {
            std::cout << "添加服务器地址为本地地址" << std::endl;
            return false;
        }

        if (m_peers.find(ip_port_) != m_peers.end())
        {
            return true;
        }

        std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(ip_port_, grpc::InsecureChannelCredentials());
        std::unique_ptr<raftRpcProctoc::raftRpc::Stub> stub = raftRpcProctoc::raftRpc::NewStub(channel);
        m_peers.insert({ip_port_, std::move(stub)});

        return true;
    }

    grpc::Status RaftService::AppendEntries(::grpc::ServerContext *context,
                                            const ::raftRpcProctoc::AppendEntriesArgs *request,
                                            ::raftRpcProctoc::AppendEntriesReply *response)
    {
        response->set_term(m_term);
        return grpc::Status::OK;
    }

    grpc::Status RaftService::InstallSnapshot(::grpc::ServerContext *context, const ::raftRpcProctoc::InstallSnapshotRequest *request, ::raftRpcProctoc::InstallSnapshotResponse *response)
    {
        response->set_term(m_term);
        return grpc::Status::OK;
    }
    grpc::Status RaftService::RequestVote(::grpc::ServerContext *context, const ::raftRpcProctoc::RequestVoteArgs *request, ::raftRpcProctoc::RequestVoteReply *response)
    {
        response->set_term(m_term);
        return grpc::Status::OK;
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
