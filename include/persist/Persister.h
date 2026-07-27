#pragma once
#include "common/LockQueue.h"
#include "common/Option.h"
#include "raftRPC.pb.h"
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace mraft
{

class Persister
{
  public:
	struct StableWaiter
	{
		std::mutex mtx;
		std::condition_variable cv;
		int pending = 0;
		bool done = false;
		bool ok = false;
	};

  private:
#pragma pack(push, 1)
	typedef struct NodeSnapshot
	{
		uint32_t node_id;
		uint32_t pid;
		char ip[32];
		short port;
		uint32_t role;
		uint32_t commit_index;
		uint32_t last_applied;
		// uint32_t last_log_index;
		// uint32_t last_log_term;
		uint32_t log_len;

		uint32_t term;

		uint32_t snapshot_index;
		uint32_t snapshot_term;

		uint32_t votedFor;
		// uint32_t updated_at_ms;

		uint32_t writedIndex = -1;

		friend std::ostream &operator<<(std::ostream &os, const NodeSnapshot &k)
		{
			os << "node_id=" << k.node_id << ", pid=" << k.pid << ", ip=" << k.ip
			   << ", port=" << k.port << ", role=" << k.role << ", commit_index=" << k.commit_index
			   << ", last_applied=" << k.last_applied << ", log_len=" << k.log_len
			   << ", term=" << k.term << ", snapshot_index=" << k.snapshot_index
			   << ", snapshot_term=" << k.snapshot_term << ", votedFor=" << k.votedFor;
			return os;
		}
	} NodeInfo;
#pragma pack(pop)

#pragma pack(push, 1)
	struct WalEntryHeader
	{
		// uint32_t magic;
		// uint32_t header_size;
		uint64_t log_index;
		uint64_t log_term;
		uint32_t log_size;
		uint32_t command_crc;
	};
#pragma pack(pop)


	// struct LockQueueElem
	// {
	// 	WalEntryHeader header;
	// 	raftRpcProctoc::LogEntry log;
	// 	std::shared_ptr<StableWaiter> waiter;
	// };


	struct LogBatch
	{
		std::vector<raftRpcProctoc::LogEntry> logs;
		std::shared_ptr<StableWaiter> waiter;
		std::function<void(bool)> callback;
	};




	NodeInfo m_nodeInfoSnapshot;
	std::string m_logs_str; // serialized m_logs vector, string make by raft node itself
	std::string m_snapshot; // serlalized snapshot in raft node


  private:
	friend class raft;
	char buf[1024 * 1024];
	constexpr static int kSyncBatchCount = 64;
	constexpr static size_t kSyncBatchBytes = 128 * 1024;
	constexpr static int kSyncIntervalMs = 1;
	constexpr static int kMaxSegmentNumEntries = 1000;


	std::string m_walDir;
	std::string m_openSegmentPath;

	uint64_t m_segmentFirstIndex = 1;
	uint64_t m_segmentLastIndex = 0;
	uint64_t m_compactedSnapshotIndex = 0;
	int m_segmentEntryCount = 0;
	size_t m_segmentBytes = 0;
	constexpr static size_t kMaxSegmentBytes = 8 * 1024 * 1024;


	// LockQueue<LockQueueElem> m_queue_log;
	LockQueue<LogBatch> m_queue_log;


	mutable std::mutex m_mtx;
	std::string m_raftState;

	std::string m_raftStateFileName;

	std::string m_logsFileName;
	int log_fd;

	std::string m_snapshotFileName;
	int snapshot_fd;

	NodeInfo *fmp = nullptr;


	bool m_isRunning;

	// long long m_raftStateSize;
	std::atomic<long long> m_raftLogsFileSize;

	std::thread persist_log_thread;

  public:
	void Save(NodeSnapshot *state_info, std::string *logs, std::string *snapshot);
	void Save(std::string, int index);

	std::string ReadSnapshot();
	std::string ReadRaftState();

	void SaveRaftState();

	long long RaftStateSize() const;

	bool AppendLogAndWaitStable(const std::vector<raftRpcProctoc::LogEntry> &logs);
	bool AppendLogAndWaitStable(const raftRpcProctoc::LogEntry &log);
	void AppendLogAsync(
	    const std::vector<raftRpcProctoc::LogEntry> &logs, std::function<void(bool)> callback);
	void AppendLogAsync(const raftRpcProctoc::LogEntry &log, std::function<void(bool)> callback);


	// 	 1. RaftStateSize() 目前如果没有正确更新，snapshot 可能永远不触发。你现在的 WAL 改造后要确认
	// m_raftStateSize 是否随着 WAL 增长更
	//      新。

	//   2. ReadSnapshot() 没有 lseek(snapshot_fd, 0, SEEK_SET)，如果 fd offset 在末尾，可能读空。见
	//   KVCache-Raft/src/persist/
	//      Persister.cc:57。

	//   3. Snapshot() 截断了内存 m_logs，但没有同步处理 WAL。重启后如果 WAL 仍然包含 snapshot
	//   前的旧日志，恢复时需要跳过 logindex <=
	//      snapshotIndex 的日志，否则内存日志和 snapshotIndex 会不一致。

	//   4. InstallSnapshot() 在持有 Raft 锁时调用 m_persister->Save(...snapshot...) 和
	//   pushMsgToKvServer，可能阻塞 Raft。保存 snapshot
	//      通常比较重，最好减少锁内 IO。

	//   5. 启动恢复顺序现在是 raft 先读 meta 和 WAL，KvServer 后读 snapshot。整体可以工作，但要保证
	//   WAL 恢复时尊重 m_lastSnapshotIndex。
	//      当前 readPersistLogs() 看起来没有过滤 snapshot 前日志。


	explicit Persister(int me, Option opts);
	~Persister();


  private:
	struct SegmentInfo
	{
		uint64_t first = 0;
		uint64_t last = 0;
		bool inprogress = false;
		std::string path;
	};

	void OpenSegment(uint64_t firstIndex);
	void RollSegment();
	std::vector<SegmentInfo> ListSegments() const;
	bool ReadWalSegment(const std::string &path, uint64_t snapshotIndex,
	    std::vector<raftRpcProctoc::LogEntry> *logs, off_t *validOffset,
	    long long *validBytes) const;
	void ReadPersistedLogs(
	    uint64_t snapshotIndex, std::vector<raftRpcProctoc::LogEntry> *logs);
	void CompactWal(uint64_t snapshotIndex);
	bool RewriteWalFrom(uint64_t firstIndex, const std::vector<raftRpcProctoc::LogEntry> &logs);
	void ReopenLatestSegment();
	void RecalculateWalSize();

	inline std::string openSegmentName(uint64_t first)
	{
		return m_walDir + "/log_inprogress_" + std::to_string(first);
	}

	inline std::string closedSegmentName(uint64_t first, uint64_t last)
	{
		return m_walDir + "/log_" + std::to_string(first) + "_" + std::to_string(last);
	}

	// void clearRaftState();
	// void clearSnapshot();
	// void clearRaftStateAndSnapshot();
};


static bool WriteAll(int fd, const void *data, size_t size)
{
	const char *p = static_cast<const char *>(data);
	while (size > 0)
	{
		ssize_t n = ::write(fd, p, size);
		if (n <= 0)
		{
			return false;
		}
		p += n;
		size -= static_cast<size_t>(n);
	}
	return true;
}

static void NotifyWaiters(
    std::vector<std::shared_ptr<mraft::Persister::StableWaiter>> &waiters, bool ok)
{
	for (auto &w : waiters)
	{
		std::lock_guard<std::mutex> lk(w->mtx);
		w->ok = ok;
		w->done = true;
		w->cv.notify_one();
	}
	waiters.clear();
}

static void NotifyCallbacks(std::vector<std::function<void(bool)>> &callbacks, bool ok)
{
	for (auto &callback : callbacks)
	{
		if (callback)
		{
			callback(ok);
		}
	}
	callbacks.clear();
}

} // namespace mraft
