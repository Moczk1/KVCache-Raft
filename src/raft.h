#pragma once
#include <memory>
#include <vector>
#include <mutex>
#include <algorithm>
#include <chrono>
#include "raftRPC.pb.h"
#include "Constant.h"
#include "Alias.h"

using raftRpcProctoc::LogEntry;

namespace mraft
{
    class raft : public raftRpcProctoc::raftRpc
    {
    private:
        /* data */
        std::mutex m_mtx;
        int m_id;

        std::vector<LogEntry> m_logs;
        int m_lastLogIndex;
        int m_lastLogTerm;
        int m_commitIndex;

        int m_lastSnapshotIndex;
        int m_lastSnapshotTerm;

        std::vector<int> m_nextIndex;
        std::vector<int> m_matchIndex;
        enum Identity
        {
            follower,
            leader,
            candidate
        };
        Identity m_state;

        enum Vote
        {
            voted,
            normal,
            Killed,
            expired
        };
        int m_currentTerm;
        Vote m_voteState;
        int m_voteForId;

        std::vector<std::shared_ptr<raftRpc::Stub>> m_peers;

        TimePoint m_lastElectionTime;
        TimePoint m_lastHearBeatTime;

    public:
        void init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
                  std::shared_ptr<LockQueue<ApplyMsg>> applyCh);

        void AppendEntries(google::protobuf::RpcController *controller,
                           const ::raftRpcProctoc::AppendEntriesArgs *request,
                           ::raftRpcProctoc::AppendEntriesReply *response,
                           ::google::protobuf::Closure *done) override;
        void InstallSnapshot(google::protobuf::RpcController *controller,
                             const ::raftRpcProctoc::InstallSnapshotRequest *request,
                             ::raftRpcProctoc::InstallSnapshotResponse *response,
                             ::google::protobuf::Closure *done) override;
        void RequestVote(google::protobuf::RpcController *controller,
                         const ::raftRpcProctoc::RequestVoteArgs *request,
                         ::raftRpcProctoc::RequestVoteReply *response,
                         ::google::protobuf::Closure *done) override;
    };

} // namespace mraft
