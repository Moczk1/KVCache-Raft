#include "raft/RaftRpcUtil.h"
#include "ioscheduler.h"
#include "raftRPC.pb.h"
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
	// 必须从 IOManager Fiber 中调用
	if (moczkrin::IOManager::GetThis() == nullptr)
	{
		if (cb)
		{
			cb(false, nullptr);
		}
		return false;
	}

	auto channel = std::make_shared<MrpcAsyncChannel>(m_asyncChannel);

	auto controller = std::make_shared<MrpcController>();

	auto reply = std::make_shared<raftRpcProctoc::AppendEntriesReply>();

	// 将业务 callback 包装成 protobuf Closure
	auto done = std::make_shared<FunctionClosure>(
	    [controller, reply, cb = std::move(cb)]() mutable
	    {
		    const bool ok = !controller->Failed();

		    if (cb)
		    {
			    cb(ok, reply);
		    }
	    });

	channel->saveCallee(controller, args, reply, done);

	raftRpcProctoc::raftRpc_Stub stub(channel.get());

	stub.AppendEntries(controller.get(), args.get(), reply.get(), nullptr);

	// 这里只表示 RPC 是否成功投递
	return !controller->Failed();
}

bool RaftRpcUtil::RequestVoteAsync(
    std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
    RequestVoteCallback cb)
{
	if (moczkrin::IOManager::GetThis() == nullptr)
	{
		return false;
	}

	auto channel = std::make_shared<MrpcAsyncChannel>(m_asyncChannel);

	auto controller = std::make_shared<MrpcController>();
	auto reply = std::make_shared<raftRpcProctoc::RequestVoteReply>();

	auto done = std::make_shared<FunctionClosure>(
	    [controller, reply, callback = std::move(cb)]() mutable
	    {
		    bool ok = !controller->Failed();

		    if (callback)
		    {
			    callback(ok, reply);
		    }
	    });

	channel->saveCallee(controller, args, reply, done);

	// Stub 只用于触发这一次 CallMethod，之后可以销毁。
	raftRpcProctoc::raftRpc_Stub stub(channel.get());

	stub.RequestVote(controller.get(), args.get(), reply.get(), nullptr);

	// 表示是否成功投递，不代表远端 RPC 已经成功。
	return !controller->Failed();
}

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

RaftRpcUtil::RaftRpcUtil(std::string ip, short port) : m_port(port), m_ip(ip)
{
	//   m_stub = new raftRpcProctoc::raftRpc_Stub(new Mrpcchannel(ip, port,
	//   true));

	Mrpcchannel *channel = new Mrpcchannel(ip, port, true);
	m_stub = std::make_shared<raftRpcProctoc::raftRpc_Stub>(channel
	    // new Mrpcchannel(ip, port, true)
	    // new MrpcAsyncChannel(ip, port, 3)
	);

	m_asyncChannel = std::make_shared<Mrpcchannel>(m_ip, m_port, false, 3);
}
// RaftRpcUtil::~RaftRpcUtil() { delete m_stub; }
} // namespace mraft
