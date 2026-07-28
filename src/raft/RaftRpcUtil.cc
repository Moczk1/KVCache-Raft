#include "raft/RaftRpcUtil.h"
#include "ioscheduler.h"
#include "raftRPC.pb.h"
#include "rpc/MrpcchannelMultiReq.h"
#include "rpc/mrpccontroller.h"
#include <cstddef>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <memory>
#include <mutex>
#include <print>
#include <sys/types.h>
#include <utility>

namespace mraft
{

bool RaftRpcUtil::AppendEntriesAsync(
    std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args, AppendEntriesCallback cb)
{
	if (moczkrin::IOManager::GetThis() == nullptr)
	{
		return false;
	}

	auto controller = std::make_shared<MrpcController>();
	auto reply = std::make_shared<raftRpcProctoc::AppendEntriesReply>();
	auto done = std::make_shared<FunctionClosure>(
	    [callback = std::move(cb), controller = controller, reply = reply]()
	    {
		    bool ok = !controller->Failed();
		    callback(ok, reply);
	    });

	auto ctx = std::make_shared<MrpcchannelMultiReq::RpcCallLifetime>();
	ctx->controller = controller;
	ctx->done = done;
	ctx->request = args;
	ctx->response = reply;

	m_channel_MR->registerCall(controller.get(), ctx);

	m_stub->AppendEntries(controller.get(), args.get(), reply.get(), done.get());

	return !controller->Failed();
}

bool RaftRpcUtil::RequestVoteAsync(
    std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args, RequestVoteCallback cb)
{
	if (moczkrin::IOManager::GetThis() == nullptr)
	{
		return false;
	}

	auto controller = std::make_shared<MrpcController>();
	// MrpcchannelMultiReq 用
	auto reply = std::make_shared<raftRpcProctoc::RequestVoteReply>();
	auto ctx = std::make_shared<MrpcchannelMultiReq::RpcCallLifetime>();
	ctx->controller = controller;
	ctx->done = std::make_shared<FunctionClosure>(
	    [callback = std::move(cb), controller, reply]()
	    {
		    bool ok = !controller->Failed();
		    callback(ok, reply);
	    });
	ctx->request = args;
	ctx->response = reply;

	m_channel_MR->registerCall(controller.get(), ctx);

	m_stub->RequestVote(controller.get(), args.get(), reply.get(), ctx->done.get());

	// 表示是否成功投递，不代表远端 RPC 已经成功。
	return !controller->Failed();
}

bool RaftRpcUtil::InstallSnapshotAsync(
    std::shared_ptr<raftRpcProctoc::InstallSnapshotRequest> args, InstallSnapshotCallback cb)
{
	if (moczkrin::IOManager::GetThis() == nullptr)
	{
		return false;
	}

	auto controller = std::make_shared<MrpcController>();
	auto reply = std::make_shared<raftRpcProctoc::InstallSnapshotResponse>();
	auto ctx = std::make_shared<MrpcchannelMultiReq::RpcCallLifetime>();

	ctx->controller = controller;
	ctx->request = args;
	ctx->response = reply;
	ctx->done = std::make_shared<FunctionClosure>(
	    [callback = std::move(cb), controller, reply]()
	    {
		    bool ok = !controller->Failed();
		    callback(ok, reply);
	    });

	m_channel_MR->registerCall(ctx->controller.get(), ctx);

	m_stub ->InstallSnapshot(ctx->controller.get(), args.get(), reply.get(), ctx->done.get());

	return !controller->Failed();
}
// 下面三个方法内部调用 stub 的 raft rpc 方法.
bool RaftRpcUtil::AppendEntries(
    raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *response)
{
	// std::unique_lock<std::mutex> lock(m_stubMtx);
	MrpcController controller;
	m_stub->AppendEntries(&controller, args, response, nullptr);
	return !controller.Failed();
}
bool RaftRpcUtil::InstallSnapshot(
    raftRpcProctoc::InstallSnapshotRequest *args, raftRpcProctoc::InstallSnapshotResponse *response)
{
	// std::unique_lock<std::mutex> lock(m_stubMtx);
	MrpcController controller;
	m_stub->InstallSnapshot(&controller, args, response, nullptr);
	return !controller.Failed();
}
bool RaftRpcUtil::RequestVote(
    raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *response)
{
	// std::unique_lock<std::mutex> lock(m_stubMtx);
	MrpcController controller;
	m_stub->RequestVote(&controller, args, response, nullptr);
	return !controller.Failed();
}

RaftRpcUtil::RaftRpcUtil(std::string ip, short port) : m_port(port), m_ip(ip)
{
	// m_stub = std::make_shared<MrpcchannelMultiReq>(
	//     new raftRpcProctoc::raftRpc_Stub(new Mrpcchannel(ip, port, true)));

	// Mrpcchannel *channel = new Mrpcchannel(ip, port, true);
	// m_stub = std::make_shared<raftRpcProctoc::raftRpc_Stub>(channel
	//     // new Mrpcchannel(ip, port, true)
	//     // new MrpcAsyncChannel(ip, port, 3)
	// );

	// m_asyncChannel = std::make_shared<Mrpcchannel>(m_ip, m_port, false, 3);

	m_channel_MR = std::make_shared<MrpcchannelMultiReq>(m_ip, m_port, true, 3);
	m_stub = std::make_shared<raftRpcProctoc::raftRpc_Stub>(m_channel_MR.get());
}
// RaftRpcUtil::~RaftRpcUtil() { delete m_stub; }
} // namespace mraft
