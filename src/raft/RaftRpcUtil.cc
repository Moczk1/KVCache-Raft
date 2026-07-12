#include "raft/RaftRpcUtil.h"
#include "rpc/mrpcchannel.h"
#include "rpc/mrpccontroller.h"
#include <memory>
#include <thread>

namespace mraft
{

bool RaftRpcUtil::AppendEntriesAsync(
    std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
    AppendEntriesCallback cb)
{
	auto self = shared_from_this();
	std::thread(
	    [self, args = std::move(args), cb = std::move(cb)]() mutable
	    {
		    auto reply = std::make_shared<raftRpcProctoc::AppendEntriesReply>();
		    bool ok = self->AppendEntries(args.get(), reply.get());
		    if (cb)
		    {
			    cb(ok, reply);
		    }
	    })
	    .detach();
	return true;
}

bool RaftRpcUtil::RequestVoteAsync(
    std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
    RequestVoteCallback cb)
{
	auto self = shared_from_this();
	std::thread(
	    [self, args = std::move(args), cb = std::move(cb)]() mutable
	    {
		    auto reply = std::make_shared<raftRpcProctoc::RequestVoteReply>();
		    bool ok = self->RequestVote(args.get(), reply.get());
		    if (cb)
		    {
			    cb(ok, reply);
		    }
	    })
	    .detach();
	return true;
}

// bool RaftRpcUtil::InstallSnapshotAsync(
//     std::shared_ptr<raftRpcProctoc::InstallSnapshotRequest> args,
//     InstallSnapshotCallback cb)
// {
// 	auto self = shared_from_this();
// 	std::thread(
// 	    [self, args = std::move(args), cb = std::move(cb)]() mutable
// 	    {
// 		    auto reply =
// 		        std::make_shared<raftRpcProctoc::InstallSnapshotResponse>();
// 		    bool ok = self->InstallSnapshot(args.get(), reply.get());
// 		    if (cb)
// 		    {
// 			    cb(ok, reply);
// 		    }
// 	    })
// 	    .detach();
// 	return true;
// }

// 下面三个方法内部调用 stub 的 raft rpc 方法.
bool RaftRpcUtil::AppendEntries(raftRpcProctoc::AppendEntriesArgs *args,
    raftRpcProctoc::AppendEntriesReply *response)
{
	std::unique_lock<std::mutex> lock(m_stubMtx);
	MrpcController controller;
	m_stub->AppendEntries(&controller, args, response, nullptr);
	return !controller.Failed();
}
bool RaftRpcUtil::InstallSnapshot(raftRpcProctoc::InstallSnapshotRequest *args,
    raftRpcProctoc::InstallSnapshotResponse *response)
{
	std::unique_lock<std::mutex> lock(m_stubMtx);
	MrpcController controller;
	m_stub->InstallSnapshot(&controller, args, response, nullptr);
	return !controller.Failed();
}
bool RaftRpcUtil::RequestVote(raftRpcProctoc::RequestVoteArgs *args,
    raftRpcProctoc::RequestVoteReply *response)
{
	std::unique_lock<std::mutex> lock(m_stubMtx);
	MrpcController controller;
	m_stub->RequestVote(&controller, args, response, nullptr);
	return !controller.Failed();
}

RaftRpcUtil::RaftRpcUtil(std::string ip, short port)
{
	//   m_stub = new raftRpcProctoc::raftRpc_Stub(new Mrpcchannel(ip, port,
	//   true));
	m_stub = std::make_unique<raftRpcProctoc::raftRpc_Stub>(
	    new Mrpcchannel(ip, port, true));
}
// RaftRpcUtil::~RaftRpcUtil() { delete m_stub; }
} // namespace mraft
