#pragma once
#include "Alias.h"
#include "ApplyMsg.h"
#include "LockQueue.h"
#include "Persister.h"
#include "RaftRpcUtil.h"
#include "raftRPC.pb.h"
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <chrono>
#include <memory>
#include <mutex>
#include <vector>

using raftRpcProctoc::LogEntry;

namespace mraft {
class raft : public raftRpcProctoc::raftRpc {
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
  enum Identity { follower, leader, candidate };
  Identity m_state;

  enum Vote { voted, normal, Killed, expired };
  int m_currentTerm;
  Vote m_voteState;
  int m_voteForId;

  std::vector<std::shared_ptr<RaftRpcUtil>> m_peers;

  TimePoint m_lastElectionTime;
  TimePoint m_lastHearBeatTime;

private:
  inline TimePoint now() { return std::chrono::system_clock::now(); }

public:
  void init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me,
            std::shared_ptr<Persister> persister,
            std::shared_ptr<LockQueue<ApplyMsg>> applyCh);

  void AppendEntries(google::protobuf::RpcController *controller,
                     const ::raftRpcProctoc::AppendEntriesArgs *request,
                     ::raftRpcProctoc::AppendEntriesReply *response,
                     ::google::protobuf::Closure *done) override;
  void AppendEntries(const ::raftRpcProctoc::AppendEntriesArgs *request,
                     ::raftRpcProctoc::AppendEntriesReply *response);
  bool sendAppendEntries(int,
                         std::shared_ptr<raftRpcProctoc::AppendEntriesArgs>,
                         std::shared_ptr<raftRpcProctoc::AppendEntriesReply>,
                         std::shared_ptr<int> appendNum);
  void doHeartBeat();
  void leaderHeartBeatTricker();

  void leaderSendSnapShot(int);
  void InstallSnapshot(google::protobuf::RpcController *controller,
                       const ::raftRpcProctoc::InstallSnapshotRequest *request,
                       ::raftRpcProctoc::InstallSnapshotResponse *response,
                       ::google::protobuf::Closure *done) override;

  void RequestVote(google::protobuf::RpcController *controller,
                   const ::raftRpcProctoc::RequestVoteArgs *request,
                   ::raftRpcProctoc::RequestVoteReply *response,
                   ::google::protobuf::Closure *done) override;

  void RequestVote(const ::raftRpcProctoc::RequestVoteArgs *request,
                   ::raftRpcProctoc::RequestVoteReply *response);
  bool sendRequestVote(int, std::shared_ptr<raftRpcProctoc::RequestVoteArgs>,
                       std::shared_ptr<raftRpcProctoc::RequestVoteReply>,
                       std::shared_ptr<int>);
  void doElection();
  void electionTimeOutTicker();

private:
  std::shared_ptr<Persister> m_persister;
  std::shared_ptr<LockQueue<ApplyMsg>> applyChan;
  int m_lastApplied; // 已经汇报给状态机（上层应用）的log 的index

  void getLastLogIndexandTerm(int &, int &);
  inline void getPrevLogInfo(int server, int &index, int &term) {
    if (m_nextIndex[server] == m_lastSnapshotIndex + 1) {
      index = m_lastSnapshotIndex;
      term = m_lastSnapshotTerm;
      return;

    } else {
      auto nextIndex = m_nextIndex[server];
      index = nextIndex - 1;
      int v_index = index - m_lastSnapshotIndex - 1;
      term = m_logs[v_index].logterm();
    }
  }
  //   inline std::vector<ApplyMsg> getApplyLogs() {
  //     std::vector<ApplyMsg> applyMsgs;
  //     myAssert(
  //         m_commitIndex <= getLastLogIndex(),
  //         format(
  //             "[func-getApplyLogs-rf{%d}] commitIndex{%d}
  //             >getLastLogIndex{%d}", m_me, m_commitIndex,
  //             getLastLogIndex()));

  //     while (m_lastApplied < m_commitIndex) {
  //       m_lastApplied++;
  //       myAssert(
  //           m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex() ==
  //               m_lastApplied,
  //           format("rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)]."
  //                  "LogIndex{%d} != rf.lastApplied{%d} ",
  //                  m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex(),
  //                  m_lastApplied));
  //       ApplyMsg applyMsg;
  //       applyMsg.CommandValid = true;
  //       applyMsg.SnapshotValid = false;
  //       applyMsg.Command =
  //           m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].command();
  //       applyMsg.CommandIndex = m_lastApplied;
  //       applyMsgs.emplace_back(applyMsg);
  //     }
  //     return applyMsgs;
  //   }

  //   inline void applierTicker() {
  //     while (true) {
  //       m_mtx.lock();
  //       if (m_state == leader) {
  //         std::print(
  //             "[Raft::applierTicker() - raft{}]
  //             m_lastApplied{}m_commitIndex{}", m_id, m_lastApplied,
  //             m_commitIndex);
  //       }
  //       auto applyMsgs = getApplyLogs();
  //       m_mtx.unlock();
  //       // 使用匿名函数是因为传递管道的时候不用拿锁
  //       //
  //       todo:好像必须拿锁，因为不拿锁的话如果调用多次applyLog函数，可能会导致应用的顺序不一样
  //       if (!applyMsgs.empty()) {
  //         DPrintf("[func- Raft::applierTicker()-raft{}] "
  //                 "向kvserver報告的applyMsgs長度爲：{}",
  //                 m_me, applyMsgs.size());
  //       }
  //       for (auto &message : applyMsgs) {
  //         applyChan->Push(message);
  //       }
  //       // usleep(1000 * ApplyInterval);
  //       sleepNMilliseconds(ApplyInterval);
  //     }
  //   }
  //   inline void readPersist(std::string data) {
  //     if (data.empty()) {
  //       return;
  //     }
  //     std::stringstream iss(data);
  //     boost::archive::text_iarchive ia(iss);
  //     // read class state from archive
  //     BoostPersistRaftNode boostPersistRaftNode;
  //     ia >> boostPersistRaftNode;

  //     m_currentTerm = boostPersistRaftNode.m_currentTerm;
  //     m_votedFor = boostPersistRaftNode.m_votedFor;
  //     m_lastSnapshotIncludeIndex =
  //         boostPersistRaftNode.m_lastSnapshotIncludeIndex;
  //     m_lastSnapshotIncludeTerm =
  //     boostPersistRaftNode.m_lastSnapshotIncludeTerm; m_logs.clear(); for
  //     (auto &item : boostPersistRaftNode.m_logs) {
  //       raftRpcProctoc::LogEntry logEntry;
  //       logEntry.ParseFromString(item);
  //       m_logs.emplace_back(logEntry);
  //     }
  //   }
};

} // namespace mraft
