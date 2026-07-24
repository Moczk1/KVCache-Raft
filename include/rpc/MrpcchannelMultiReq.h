#pragma once
#include "ioscheduler.h"
#include "raftRPC.pb.h"
#include "rpc/MrpcchannelMultiReq.h"
#include "rpc/mrpcchannel.h"
#include "rpc/mrpccontroller.h"
#include "rpcheader.pb.h"
#include <arpa/inet.h>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <google/protobuf/stubs/callback.h>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

namespace mraft
{
using google::protobuf::Closure;
using google::protobuf::Message;
using google::protobuf::MethodDescriptor;
using google::protobuf::RpcController;
// 另一种 channel 的实现， Utils 的接口和调用不变，更换底层的 channel 通信
class MrpcchannelMultiReq
    : public google::protobuf::RpcChannel,
      public std::enable_shared_from_this<MrpcchannelMultiReq>
{
	using ControllerPtr = std::shared_ptr<RpcController>;
	using MessagePtr = std::shared_ptr<Message>;
	using ClosurePtr = std::shared_ptr<Closure>;
	using AppendEntriesCallback = std::function<void(
	    bool ok, std::shared_ptr<raftRpcProctoc::AppendEntriesReply>)>;
	using RequestVoteCallback = std::function<void(
	    bool ok, std::shared_ptr<raftRpcProctoc::RequestVoteReply>)>;

  public:
	MrpcchannelMultiReq(const std::string &, const short,
	    bool connectNow = true, int retry = 3);

	// void CallMethodAsync(const MethodDescriptor *method,
	//     std::shared_ptr<RpcController> controller,
	//     std::shared_ptr<const Message> request,
	//     std::shared_ptr<Message> response, std::shared_ptr<Closure> done);

	void CallMethod(const MethodDescriptor *method, RpcController *controller,
	    const Message *request, Message *response, Closure *done) override;
	// void saveCallee(ControllerPtr controller, MessagePtr request,
	//     MessagePtr response, ClosurePtr done = nullptr);

	bool newConnect(const char *ip, uint16_t port, std::string *errMsg);



	
  public:
	std::mutex lifetime_mutex;
	struct RpcCallLifetime
	{
		std::shared_ptr<RpcController> controller;
		std::shared_ptr<Message> request;
		std::shared_ptr<Message> response;
		std::shared_ptr<google::protobuf::Closure> done;
	};
	void registerCall(
	    RpcController *controller, std::shared_ptr<RpcCallLifetime> ctx);
	std::unordered_map<RpcController *, std::shared_ptr<RpcCallLifetime>>
	    m_rpc_calllifetime;




  private:
	std::mutex m_sendMutex;
	bool sendAll(const char *data, size_t size, std::string *errMsg);

	void recvLoop();
	bool recvVarint32(uint32_t &value);
	bool recvExact(void *buffer, size_t size);
	void failAllPending(const std::string &);
	void completeResponse(const RPC::RpcResponseFrame &frame);

	inline static void appendVarint32(uint32_t value, std::string &out)
	{
		while (value >= 0x80)
		{
			out.push_back(static_cast<char>((value & 0x7f) | 0x80));
			value >>= 7;
		}

		out.push_back(static_cast<char>(value));
	}

	inline static constexpr uint32_t kMaxFrameSize = 16 * 1024 * 1024; // 16MB

  public:
	struct PendingCall
	{
		std::shared_ptr<RpcController> controll = nullptr;
		std::shared_ptr<Message> response = nullptr;
		std::shared_ptr<Closure> done = nullptr;

		uint64_t requestId = 0;
	};
	std::atomic<uint64_t> m_nextRequset_id = {0};
	std::mutex m_pendingMutex;
	std::unordered_map<uint64_t, PendingCall> m_pendings;

  private:
	std::unique_ptr<moczkrin::IOManager> m_sender;
	std::unique_ptr<moczkrin::IOManager> m_recipient;
	std::unique_ptr<moczkrin::IOManager> m_worker;

	std::atomic<bool> m_stopped{false};

	int m_clientFd;
	const std::string m_ip;
	const short m_port;
	int m_retry = 3;
};
} // namespace mraft
