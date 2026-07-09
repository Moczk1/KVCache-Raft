#pragma once
#include "Alias.h"
#include "ApplyMsg.h"
#include "LockQueue.h"
#include "Persister.h"
#include "RaftRpcUtil.h"
#include "raftRPC.pb.h"
#include "util.h"
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/access.hpp>
#include <chrono>
#include <memory>
#include <mutex>
#include <vector>

#include "../coroutine/include/ioscheduler.h"

using raftRpcProctoc::LogEntry;

namespace mraft
{
class raft : public raftRpcProctoc::raftRpc
{
  private:
	/* data */
	mutable std::mutex m_mtx;
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

	std::vector<std::shared_ptr<RaftRpcUtil>> m_peers;

	TimePoint m_lastElectionTime;
	TimePoint m_lastHearBeatTime;

  private:
	inline TimePoint now() { return std::chrono::system_clock::now(); }

  public:
	void init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me,
	    std::shared_ptr<Persister> persister,
	    std::shared_ptr<LockQueue<ApplyMsg>> applyCh);

	// AE
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

	// snapshot
	void leaderSendSnapShot(int);
	void InstallSnapshot(const ::raftRpcProctoc::InstallSnapshotRequest *,
	    ::raftRpcProctoc::InstallSnapshotResponse *);
	void InstallSnapshot(google::protobuf::RpcController *controller,
	    const ::raftRpcProctoc::InstallSnapshotRequest *request,
	    ::raftRpcProctoc::InstallSnapshotResponse *response,
	    ::google::protobuf::Closure *done) override;
	void pushMsgToKvServer(ApplyMsg);
	void Snapshot(int index, std::string snapshot);

	// vote
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

	// 状态
	// void leaderUpdateCommitIndex();

	/** 持久化 */
  private:
	std::shared_ptr<Persister> m_persister;

	inline void persist()
	{
		auto data = persistData();
		m_persister->SaveRaftState(data);
	}
	void readPersist(std::string data);
	std::string persistData();

	/** 客户端通信 */
  private:
	std::shared_ptr<LockQueue<ApplyMsg>> applyChan;
	int m_lastApplied; // 已经汇报给状态机（上层应用）的log 的index

	void applierTicker();
	std::vector<ApplyMsg> getApplyLogs();

  public:
	void Start(Op op, int &index, int &term, bool &isLeader);
	int GetRaftStateSize();

  public:
	inline void GetState(int *term, bool *isLeader) const
	{
		std::unique_lock<std::mutex> lock(m_mtx);
		*term = m_currentTerm;
		*isLeader = (m_state == leader);
		lock.unlock();
	}

  private:
	std::unique_ptr<moczkrin::IOManager> m_ioManager = nullptr;

	inline int getLastLogTerm()
	{
		getLastLogIndexandTerm(m_lastLogIndex, m_lastLogTerm);
		return m_lastLogTerm;
	}

	inline int getLastLogIndex()
	{
		getLastLogIndexandTerm(m_lastLogIndex, m_lastLogTerm);
		return m_lastLogIndex;
	}

	inline void getLastLogIndexandTerm(int &index, int &term)
	{
		if (m_logs.empty())
		{
			index = m_lastSnapshotIndex;
			term = m_lastSnapshotTerm;
			return;
		}
		else
		{
			int len = m_logs.size();
			index = m_logs[len - 1].logindex();
			term = m_logs[len - 1].logterm();
			return;
		}
	}

	inline void getPrevLogInfo(int server, int &index, int &term)
	{
		if (m_nextIndex[server] == m_lastSnapshotIndex + 1)
		{
			index = m_lastSnapshotIndex;
			term = m_lastSnapshotTerm;
			return;
		}
		else
		{
			auto nextIndex = m_nextIndex[server];
			index = nextIndex - 1;
			int v_index = index - m_lastSnapshotIndex - 1;
			term = m_logs[v_index].logterm();
		}
	}

	class BoostPersistRaftNode
	{
	  public:
		friend class boost::serialization::access;

		template <class T> void serialize(T &ar, const unsigned int version)
		{
			ar & m_currentTerm;
			ar & m_votedFor;
			ar & m_lastSnapshotIncludeIndex;
			ar & m_lastSnapshotIncludeTerm;
			ar & m_logs;
		}

		int m_currentTerm;
		int m_votedFor;
		int m_lastSnapshotIncludeIndex;
		int m_lastSnapshotIncludeTerm;
		std::vector<std::string> m_logs;
		std::unordered_map<std::string, int> umap;
	};

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
};

} // namespace mraft
