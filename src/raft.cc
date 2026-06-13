#include "raft.h"

using ::AppendEntriesArgs;
using ::AppendEntriesReply;
using ::LogEntry;
using ::RequestVoteArgs;
using ::RequestVoteReply;

namespace moczkrin
{

    ::grpc::Status RaftService::AppendEntries(::grpc::ServerContext *context,
                                              const ::AppendEntriesArgs *request,
                                              ::AppendEntriesReply *response)
    {

        return grpc::Status::OK;
    }

    ::grpc::Status RaftService::InstallSnapshot(::grpc::ServerContext *context,
                                                const ::InstallSnapshotRequest *request,
                                                ::InstallSnapshotResponse *response)
    {
        return grpc::Status::OK;
    }

    grpc::Status RaftService::RequestVote(::grpc::ServerContext *context,
                                          const RequestVoteArgs *request,
                                          RequestVoteReply *response)
    {
        return grpc::Status::OK;
    }

    void RaftService::init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me)
    {
        this->m_peers = peers;
        m_id = me;

        m_ioManager->scheduleLock([this]() -> void
                                  { this->leaderHearBeatTicker(); })
    }

    void RaftService::leaderHearBeatTicker()
    {
        while (true)
        {
            for(const auto& stub: m_peers)
            {
                stub->AppendEntries()
            }
        }
    }

}; // RaftService