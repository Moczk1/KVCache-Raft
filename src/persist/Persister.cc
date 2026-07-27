#include "persist/Persister.h"
#include "common/Option.h"
#include "common/util.h"
#include <algorithm>
#include <cerrno>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iterator>
#include <mutex>
#include <print>
#include <string>
#include <system_error>

namespace mraft
{

// 会涉及反复打开文件的操作，没有考虑如果文件出现问题会怎么办？
// ？
void Persister::Save(NodeSnapshot *state_info, std::string *logs, std::string *snapshot)
{
	if (state_info != nullptr)
	{
		msync(fmp, sizeof(NodeInfo), MS_ASYNC);
		if (fmp->snapshot_index > m_compactedSnapshotIndex)
		{
			CompactWal(fmp->snapshot_index);
		}
	}

	// if (logs != nullptr)
	// {
	// 	Save(*logs, 1);
	// }

	if (snapshot != nullptr)
	{
		Save(*snapshot, 2);
	}
}

void Persister::Save(std::string str, int index)
{
	size_t size = str.size();

	switch (index)
	{
	case 1:
		// lseek(log_fd, 0, SEEK_SET);
		// ::write(log_fd, str.data(), size);
		// ftruncate(log_fd, size);
		// fdatasync(log_fd);
		break;
	case 2:
	{
		std::string tmp = m_snapshotFileName + ".tmp";
		int fd = ::open(tmp.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
		if (fd < 0)
		{
			perror("open snapshot tmp failed");
			return;
		}

		bool ok = WriteAll(fd, str.data(), size);
		if (ok)
		{
			ok = (::fdatasync(fd) == 0);
		}
		::close(fd);

		if (!ok)
		{
			::unlink(tmp.c_str());
			perror("write snapshot failed");
			return;
		}

		if (::rename(tmp.c_str(), m_snapshotFileName.c_str()) != 0)
		{
			::unlink(tmp.c_str());
			perror("rename snapshot failed");
			return;
		}

		int dir_fd = ::open(".", O_RDONLY | O_DIRECTORY);
		if (dir_fd >= 0)
		{
			::fsync(dir_fd);
			::close(dir_fd);
		}

		if (snapshot_fd >= 0)
		{
			::close(snapshot_fd);
		}
		snapshot_fd = open(m_snapshotFileName.c_str(), O_RDWR | O_CREAT, 0644);
		break;
	}
	default:
		return;
	}
}

std::string Persister::ReadSnapshot()
{
	std::ifstream ifs(m_snapshotFileName, std::ios::binary);
	if (!ifs.is_open())
	{
		std::cerr << "打开文件失败！" << std::endl;
		exit(11);
	}
	return std::string(std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>());
}

std::string Persister::ReadRaftState()
{
	std::ifstream file(m_raftStateFileName, std::ios::binary);
	if (!file.is_open())
	{
		std::cerr << "打开文件失败！" << std::endl;
		exit(11);
	}
	::memset(&m_nodeInfoSnapshot, 0, sizeof(NodeInfo));
	file.read(reinterpret_cast<char *>(&m_nodeInfoSnapshot), sizeof(NodeInfo));
	return std::string();
}

void Persister::SaveRaftState() { Save(&m_nodeInfoSnapshot, nullptr, nullptr); }

long long Persister::RaftStateSize() const { return m_raftLogsFileSize.load(); }


Persister::Persister(int me, Option opts) : m_raftLogsFileSize(0), m_isRunning(true)
{

	m_walDir = "raftNode_" + std::to_string(me) + "_wal";

	std::error_code ec;
	if (!std::filesystem::create_directories(m_walDir, ec) && ec)
	{
		std::cerr << "Failed to create wal directory: " << ec.message() << "\n";
	}

	log_fd = -1;


	m_raftStateFileName = "raftNode_" + std::to_string(me) + "_info.txt";
	m_logsFileName = "raftNode_" + std::to_string(me) + "_logs.txt";
	m_snapshotFileName = "raftNode_" + std::to_string(me) + "_snapshot.txt";

	int raft_file = open(m_raftStateFileName.c_str(), O_RDWR | O_CREAT, 0644);
	if (raft_file == -1)
	{
		perror("open failed");
		exit(1);
	}

	if (ftruncate(raft_file, sizeof(NodeInfo)) == -1)
	{
		perror("ftruncate failed");
		close(raft_file);
		exit(2);
	}

	fmp =
	    (NodeInfo *)mmap(NULL, sizeof(NodeInfo), PROT_READ | PROT_WRITE, MAP_SHARED, raft_file, 0);

	close(raft_file);

	if (fmp == MAP_FAILED)
	{
		perror("mmap failed");
		close(raft_file);
		exit(3);
	}
	close(raft_file);

	// log_fd = open(m_logsFileName.c_str(), O_RDWR | O_CREAT, 0644);
	// if (log_fd == -1)
	// {
	// 	perror("open failed");
	// 	exit(1);
	// }
	// lseek(log_fd, 0, SEEK_END);

	snapshot_fd = open(m_snapshotFileName.c_str(), O_RDWR | O_CREAT, 0644);
	if (snapshot_fd == -1)
	{
		perror("open failed");
		exit(1);
	}

	ReopenLatestSegment();
	RecalculateWalSize();


	auto task = [this]()
	{
		size_t pending_bytes = 0;
		int pending_count = 0;
		auto last_sync = std::chrono::steady_clock::now();

		std::vector<std::shared_ptr<StableWaiter>> waiters;
		std::vector<std::function<void(bool)>> callbacks;

		while (m_isRunning)
		{
			LogBatch batch;

			// version 1. each time pick one batch to wirte into and fsync into disk file.
			// performance is slow.
			bool ok = m_queue_log.timeOutPop(kSyncIntervalMs, &batch);

			std::unique_lock<std::mutex> wal_lock(m_mtx);
			off_t batch_start_offset = ::lseek(log_fd, 0, SEEK_CUR);

			if (ok)
			{
				auto first = std::move(batch);
				std::vector<LogBatch> batches;
				batches.push_back(std::move(first));

				m_queue_log.TryPopBulk(&batches, kSyncBatchCount - 1);

				bool batch_write_ok = true;
				bool need_roll = false;

				const auto old_segment_last_index = m_segmentLastIndex;
				const auto old_segment_entry_count = m_segmentEntryCount;
				const auto old_segment_bytes = m_segmentBytes;
				size_t written_bytes = 0;
				int written_count = 0;

				for (auto &currentBatch : batches)
				{
					if (currentBatch.waiter)
					{
						waiters.push_back(currentBatch.waiter);
					}
					if (currentBatch.callback)
					{
						callbacks.push_back(std::move(currentBatch.callback));
					}

					for (const auto &log : currentBatch.logs)
					{
						std::string body;
						log.SerializePartialToString(&body);

						WalEntryHeader header{};
						header.log_size = body.size();
						header.log_index = log.logindex();
						header.log_term = log.logterm();
						header.command_crc = 0;

						bool write_ok = WriteAll(log_fd, &header, sizeof(header)) &&
						                WriteAll(log_fd, body.data(), body.size());

						if (!write_ok)
						{
							batch_write_ok = false;
							break;
						}

						const size_t entry_size = sizeof(header) + body.size();
						written_bytes += entry_size;
						written_count++;

						m_segmentLastIndex = header.log_index;
						m_segmentEntryCount++;
						m_segmentBytes += entry_size;

						if (m_segmentEntryCount >= kMaxSegmentNumEntries ||
						    m_segmentBytes >= kMaxSegmentBytes)
						{
							need_roll = true;
						}
					}

					if (!batch_write_ok)
					{
						break;
					}
				}

				if (!batch_write_ok)
				{
					::ftruncate(log_fd, batch_start_offset);
					::lseek(log_fd, batch_start_offset, SEEK_SET);
					::fdatasync(log_fd);

					m_segmentLastIndex = old_segment_last_index;
					m_segmentEntryCount = old_segment_entry_count;
					m_segmentBytes = old_segment_bytes;

					NotifyWaiters(waiters, false);
					NotifyCallbacks(callbacks, false);
					pending_count = 0;
					pending_bytes = 0;
					continue;
				}

				pending_count += written_count;
				pending_bytes += written_bytes;
				m_raftLogsFileSize.fetch_add(written_bytes);

				if (need_roll)
				{
					bool sync_ok = (::fdatasync(log_fd) == 0);
					if (sync_ok)
					{
						RollSegment();
					}

					pending_count = 0;
					pending_bytes = 0;
					last_sync = std::chrono::steady_clock::now();

					NotifyWaiters(waiters, sync_ok);
					NotifyCallbacks(callbacks, sync_ok);
					continue;
				}
			}

			auto now = std::chrono::steady_clock::now();
			bool time_due = now - last_sync > std::chrono::milliseconds(kSyncIntervalMs);

			bool should_sync =
			    pending_count > 0 &&
			    (pending_count >= kSyncBatchCount || pending_bytes >= kSyncBatchBytes || time_due);

			if (should_sync)
			{
				bool sync_ok = (::fdatasync(log_fd) == 0);

				pending_count = 0;
				pending_bytes = 0;
				last_sync = std::chrono::steady_clock::now();

				NotifyWaiters(waiters, sync_ok);
				NotifyCallbacks(callbacks, sync_ok);
			}
			wal_lock.unlock();
		}

		if (pending_count > 0)
		{
			std::lock_guard<std::mutex> wal_lock(m_mtx);
		bool sync_ok = (::fdatasync(log_fd) == 0);
		NotifyWaiters(waiters, sync_ok);
		NotifyCallbacks(callbacks, sync_ok);
	}
	};
	persist_log_thread = std::thread(task);
}

bool Persister::AppendLogAndWaitStable(const std::vector<raftRpcProctoc::LogEntry> &logs)
{
	if (logs.empty())
	{
		return true;
	}

	auto waiter = std::make_shared<StableWaiter>();

	LogBatch batch;
	batch.logs = logs;
	batch.waiter = waiter;

	m_queue_log.Push(std::move(batch));

	std::unique_lock<std::mutex> lk(waiter->mtx);
	waiter->cv.wait(lk, [&] { return waiter->done; });

	return waiter->ok;
}
bool Persister::AppendLogAndWaitStable(const raftRpcProctoc::LogEntry &log)
{
	std::vector<raftRpcProctoc::LogEntry> logs;
	logs.push_back(log);
	return AppendLogAndWaitStable(logs);
}

void Persister::AppendLogAsync(
    const std::vector<raftRpcProctoc::LogEntry> &logs, std::function<void(bool)> callback)
{
	if (logs.empty())
	{
		if (callback)
		{
			callback(true);
		}
		return;
	}

	LogBatch batch;
	batch.logs = logs;
	batch.callback = std::move(callback);

	m_queue_log.Push(std::move(batch));
}

void Persister::AppendLogAsync(
    const raftRpcProctoc::LogEntry &log, std::function<void(bool)> callback)
{
	std::vector<raftRpcProctoc::LogEntry> logs;
	logs.push_back(log);
	AppendLogAsync(logs, std::move(callback));
}


Persister::~Persister()
{
	m_isRunning = false;

	if (persist_log_thread.joinable())
	{
		persist_log_thread.join();
	}

	::close(log_fd);
	::close(snapshot_fd);
	munmap(fmp, sizeof(NodeInfo));
}

void Persister::OpenSegment(uint64_t firstIndex)
{
	if (log_fd >= 0)
	{
		::close(log_fd);
	}

	m_segmentFirstIndex = firstIndex;
	m_segmentLastIndex = firstIndex - 1;
	m_segmentEntryCount = 0;
	m_segmentBytes = 0;

	m_openSegmentPath = openSegmentName(firstIndex);
	log_fd = ::open(m_openSegmentPath.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
	if (log_fd < 0)
	{
		perror("open wal segment failed");
		exit(1);
	}
}

void Persister::RollSegment()
{
	if (log_fd < 0 || m_segmentEntryCount == 0)
	{
		return;
	}

	::fdatasync(log_fd);
	::close(log_fd);
	log_fd = -1;

	std::string closed = closedSegmentName(m_segmentFirstIndex, m_segmentLastIndex);
	if (::rename(m_openSegmentPath.c_str(), closed.c_str()) != 0)
	{
		perror("rename wal segment failed");
		exit(1);
	}

	OpenSegment(m_segmentLastIndex + 1);
}

std::vector<Persister::SegmentInfo> Persister::ListSegments() const
{
	std::vector<SegmentInfo> segments;
	std::error_code ec;
	for (const auto &entry : std::filesystem::directory_iterator(m_walDir, ec))
	{
		if (ec || !entry.is_regular_file())
		{
			continue;
		}

		const std::string name = entry.path().filename().string();
		SegmentInfo info;
		info.path = entry.path().string();

		if (name.rfind("log_inprogress_", 0) == 0)
		{
			info.inprogress = true;
			info.first = std::stoull(name.substr(strlen("log_inprogress_")));
			info.last = UINT64_MAX;
			segments.push_back(info);
			continue;
		}

		if (name.rfind("log_", 0) == 0)
		{
			size_t split = name.find('_', strlen("log_"));
			if (split == std::string::npos)
			{
				continue;
			}
			info.first = std::stoull(name.substr(strlen("log_"), split - strlen("log_")));
			info.last = std::stoull(name.substr(split + 1));
			segments.push_back(info);
		}
	}

	std::sort(segments.begin(), segments.end(),
	    [](const SegmentInfo &a, const SegmentInfo &b)
	    {
		    if (a.first != b.first)
		    {
			    return a.first < b.first;
		    }
		    return a.inprogress < b.inprogress;
	    });
	return segments;
}

bool Persister::ReadWalSegment(const std::string &path, uint64_t snapshotIndex,
    std::vector<raftRpcProctoc::LogEntry> *logs, off_t *validOffset, long long *validBytes) const
{
	int fd = ::open(path.c_str(), O_RDWR);
	if (fd < 0)
	{
		return false;
	}

	off_t offset = 0;
	long long bytes = 0;
	while (true)
	{
		WalEntryHeader header{};
		ssize_t n = ::read(fd, &header, sizeof(header));
		if (n == 0)
		{
			break;
		}
		if (n != static_cast<ssize_t>(sizeof(header)))
		{
			break;
		}
		if (header.log_size == 0 || header.log_size > 16 * 1024 * 1024)
		{
			break;
		}

		std::string body(header.log_size, '\0');
		ssize_t read_n = ::read(fd, body.data(), body.size());
		if (read_n != static_cast<ssize_t>(body.size()))
		{
			break;
		}

		raftRpcProctoc::LogEntry entry;
		if (!entry.ParseFromString(body))
		{
			break;
		}
		if (entry.logindex() != static_cast<int32_t>(header.log_index) ||
		    entry.logterm() != static_cast<int32_t>(header.log_term))
		{
			break;
		}

		const off_t entrySize = sizeof(header) + header.log_size;
		offset += entrySize;
		bytes += entrySize;
		if (header.log_index > snapshotIndex)
		{
			logs->push_back(std::move(entry));
		}
	}

	::ftruncate(fd, offset);
	::close(fd);

	if (validOffset != nullptr)
	{
		*validOffset = offset;
	}
	if (validBytes != nullptr)
	{
		*validBytes = bytes;
	}
	return true;
}

void Persister::ReadPersistedLogs(
    uint64_t snapshotIndex, std::vector<raftRpcProctoc::LogEntry> *logs)
{
	std::lock_guard<std::mutex> lock(m_mtx);

	std::vector<raftRpcProctoc::LogEntry> replay;
	long long validBytes = 0;
	for (const auto &segment : ListSegments())
	{
		long long segmentBytes = 0;
		ReadWalSegment(segment.path, snapshotIndex, &replay, nullptr, &segmentBytes);
		validBytes += segmentBytes;
	}

	logs->clear();
	for (const auto &entry : replay)
	{
		if (entry.logindex() <= static_cast<int32_t>(snapshotIndex))
		{
			continue;
		}

		if (logs->empty())
		{
			if (entry.logindex() != static_cast<int32_t>(snapshotIndex + 1))
			{
				continue;
			}
			logs->push_back(entry);
			continue;
		}

		const int expectedNext = logs->back().logindex() + 1;
		if (entry.logindex() == expectedNext)
		{
			logs->push_back(entry);
		}
		else if (entry.logindex() <= logs->back().logindex())
		{
			const int vecIndex = entry.logindex() - static_cast<int>(snapshotIndex) - 1;
			if (vecIndex >= 0 && vecIndex < static_cast<int>(logs->size()))
			{
				logs->erase(logs->begin() + vecIndex, logs->end());
				logs->push_back(entry);
			}
		}
	}

	m_raftLogsFileSize.store(validBytes);
}

void Persister::CompactWal(uint64_t snapshotIndex)
{
	std::lock_guard<std::mutex> lock(m_mtx);

	std::vector<raftRpcProctoc::LogEntry> logs;
	for (const auto &segment : ListSegments())
	{
		ReadWalSegment(segment.path, snapshotIndex, &logs, nullptr, nullptr);
	}

	std::vector<raftRpcProctoc::LogEntry> compacted;
	for (const auto &entry : logs)
	{
		if (entry.logindex() <= static_cast<int32_t>(snapshotIndex))
		{
			continue;
		}
		if (compacted.empty())
		{
			if (entry.logindex() == static_cast<int32_t>(snapshotIndex + 1))
			{
				compacted.push_back(entry);
			}
			continue;
		}

		const int expectedNext = compacted.back().logindex() + 1;
		if (entry.logindex() == expectedNext)
		{
			compacted.push_back(entry);
		}
		else if (entry.logindex() <= compacted.back().logindex())
		{
			const int vecIndex = entry.logindex() - static_cast<int>(snapshotIndex) - 1;
			if (vecIndex >= 0 && vecIndex < static_cast<int>(compacted.size()))
			{
				compacted.erase(compacted.begin() + vecIndex, compacted.end());
				compacted.push_back(entry);
			}
		}
	}

	const uint64_t firstIndex =
	    compacted.empty() ? snapshotIndex + 1 : static_cast<uint64_t>(compacted.front().logindex());
	RewriteWalFrom(firstIndex, compacted);
	m_compactedSnapshotIndex = snapshotIndex;
}

bool Persister::RewriteWalFrom(
    uint64_t firstIndex, const std::vector<raftRpcProctoc::LogEntry> &logs)
{
	if (log_fd >= 0)
	{
		::close(log_fd);
		log_fd = -1;
	}

	for (const auto &segment : ListSegments())
	{
		::unlink(segment.path.c_str());
	}

	OpenSegment(firstIndex);
	m_raftLogsFileSize.store(0);

	for (const auto &log : logs)
	{
		std::string body;
		log.SerializePartialToString(&body);

		WalEntryHeader header{};
		header.log_size = body.size();
		header.log_index = log.logindex();
		header.log_term = log.logterm();
		header.command_crc = 0;

		if (!WriteAll(log_fd, &header, sizeof(header)) ||
		    !WriteAll(log_fd, body.data(), body.size()))
		{
			return false;
		}

		const size_t entrySize = sizeof(header) + body.size();
		m_raftLogsFileSize.fetch_add(entrySize);
		m_segmentLastIndex = header.log_index;
		m_segmentEntryCount++;
		m_segmentBytes += entrySize;
	}

	::fdatasync(log_fd);
	return true;
}

void Persister::ReopenLatestSegment()
{
	auto segments = ListSegments();
	if (segments.empty())
	{
		OpenSegment(1);
		return;
	}

	const SegmentInfo latest = segments.back();
	if (!latest.inprogress)
	{
		OpenSegment(latest.last + 1);
		return;
	}

	std::vector<raftRpcProctoc::LogEntry> logs;
	off_t validOffset = 0;
	long long validBytes = 0;
	ReadWalSegment(latest.path, 0, &logs, &validOffset, &validBytes);

	m_segmentFirstIndex = latest.first;
	m_segmentLastIndex = logs.empty() ? latest.first - 1 : logs.back().logindex();
	m_segmentEntryCount = logs.size();
	m_segmentBytes = validBytes;
	m_openSegmentPath = latest.path;

	log_fd = ::open(m_openSegmentPath.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
	if (log_fd < 0)
	{
		perror("open wal segment failed");
		exit(1);
	}
}

void Persister::RecalculateWalSize()
{
	long long bytes = 0;
	std::error_code ec;
	for (const auto &entry : std::filesystem::directory_iterator(m_walDir, ec))
	{
		if (!ec && entry.is_regular_file())
		{
			bytes += static_cast<long long>(entry.file_size(ec));
		}
	}
	m_raftLogsFileSize.store(bytes);
}


} // namespace mraft
