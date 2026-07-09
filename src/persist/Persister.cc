#include "persist/Persister.h"
#include "common/Option.h"
#include "common/util.h"
#include <fstream>
#include <ios>
#include <mutex>
#include <print>
#include <string>

namespace mraft
{

// 会涉及反复打开文件的操作，没有考虑如果文件出现问题会怎么办？？
void Persister::Save(std::string raftstate, std::string snapshot)
{
	std::unique_lock<std::mutex> lock(m_mtx);
	clearRaftStateAndSnapshot();

	m_raftStateOutStream << raftstate;
	m_snapshotOutStream << snapshot;
}

std::string Persister::ReadSnapshot()
{
	std::unique_lock<std::mutex> lock(m_mtx);

	if (m_snapshotOutStream.is_open())
	{
		m_snapshotOutStream.close();
	}

	DeferClass defer(
	    [this] { this->m_snapshotOutStream.open(m_snapshotFileName); });

	std::fstream ifs(m_snapshotFileName, std::ios_base::in);

	if (!ifs.good())
	{
		return "";
	}

	std::string snapshot;

	ifs >> snapshot;

	ifs.close();

	return snapshot;
}

void Persister::SaveRaftState(const std::string &data)
{
	std::unique_lock<std::mutex> lock(m_mtx);

	clearRaftState();
	m_raftStateOutStream << data;

	m_raftStateSize += data.size();
}

long long Persister::RaftStateSize() const
{
	std::unique_lock<std::mutex> lock(m_mtx);
	return m_raftStateSize;
}

std::string Persister::ReadRaftState()
{
	std::unique_lock<std::mutex> lock(m_mtx);

	std::fstream ifs(m_raftStateFileName, std::ios::in);
	if (!ifs.good())
	{
		return "";
	}

	std::string snapshot;
	ifs >> snapshot;

	ifs.close();

	m_raftStateSize = snapshot.size();
	return snapshot;
}

Persister::Persister(int me, Option opts) : m_raftStateSize(0)
{
	m_raftStateFileName = opts.m_raftFileName;
	m_snapshotFileName = opts.m_snapshotFileName;

	bool fileOpenFlag = true;

	std::fstream file(m_raftStateFileName, std::ios::out | std::ios::trunc);

	if (file.is_open())
	{
		file.close();
	}
	else
	{
		fileOpenFlag = false;
	}

	file = std::fstream(m_snapshotFileName, std::ios::out | std::ios::trunc);
	if (file.is_open())
		file.close();
	else
		fileOpenFlag = false;

	if (!fileOpenFlag)
	{
		std::print(
		    "{}{}:\t\tPersister file open error!\n", __FUNCTION__, __LINE__);
	}

	m_raftStateOutStream.open(m_raftStateFileName);
	m_snapshotOutStream.open(m_snapshotFileName);
}

Persister::~Persister()
{
	if (m_raftStateOutStream.is_open())
	{
		m_raftStateOutStream.close();
	}

	if (m_snapshotOutStream.is_open())
		m_snapshotOutStream.close();
}

void Persister::clearRaftState()
{
	m_raftStateSize = 0;
	// 关闭文件流
	if (m_raftStateOutStream.is_open())
	{
		m_raftStateOutStream.close();
	}
	// 重新打开文件流并清空文件内容
	m_raftStateOutStream.open(
	    m_raftStateFileName, std::ios::out | std::ios::trunc);
}

void Persister::clearSnapshot()
{
	if (m_snapshotOutStream.is_open())
	{
		m_snapshotOutStream.close();
	}
	m_snapshotOutStream.open(
	    m_snapshotFileName, std::ios::out | std::ios::trunc);
}

void Persister::clearRaftStateAndSnapshot()
{
	clearRaftState();
	clearSnapshot();
}

} // namespace mraft