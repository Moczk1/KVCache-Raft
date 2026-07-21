#pragma once
#include "ioscheduler.h"
#include "rpcheader.pb.h"
#include "scheduler.h"
#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <format>
#include <functional>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <google/protobuf/stubs/callback.h>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <print>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_map>
#include <utility>

namespace moczkrin
{
class Fiber;
class Scheduler;
} // namespace moczkrin

namespace mraft
{
using google::protobuf::Closure;
using google::protobuf::Message;
using google::protobuf::MethodDescriptor;
using google::protobuf::RpcController;

class Mrpcchannel : public google::protobuf::RpcChannel
{

  public:
	// 同步入口
	void CallMethod(const MethodDescriptor *method, RpcController *controller,
	    const Message *request, Message *response, Closure *done) override;

	Mrpcchannel(std::string ip, short port, bool connectNow, int retry = 3);
	~Mrpcchannel() override;

	Mrpcchannel(const Mrpcchannel &) = delete;
	Mrpcchannel &operator=(const Mrpcchannel &) = delete;


	std::atomic<uint64_t> m_request_id{0};

  private:
	int m_clientFd;
	const std::string m_ip;
	const uint16_t m_port;
	const int m_retryCount = 3;
	bool newConnect(const char *ip, uint16_t prot, std::string *errMsg);
	void CallMethodImpl1(const MethodDescriptor *method, RpcController *controller,
	    const Message *request, Message *response);
	void CallMethodImplFrame(const MethodDescriptor *method, RpcController *controller,
	    const Message *request, Message *response);
};

/**
 * 基于 Fiber + Scheduler 的异步 RPC 通道。
 *
 * 每个 MrpcAsyncChannel 只承载一次 RPC。调用 CallMethod 前必须先调用
 * saveCallee，以 shared_ptr 保证异步期间参数和回调的生命周期。
 * CallMethod 与 wait 必须在 IOManager 调度的 Fiber 中执行。
 */
class MrpcAsyncChannel final : public google::protobuf::RpcChannel,
                               public std::enable_shared_from_this<MrpcAsyncChannel>
{
  public:
	using ptr = std::shared_ptr<MrpcAsyncChannel>;
	using ControllerPtr = std::shared_ptr<RpcController>;
	using MessagePtr = std::shared_ptr<Message>;
	using ClosurePtr = std::shared_ptr<Closure>;

	// MrpcAsyncChannel(std::string ip, short port, int retry = 3);
	MrpcAsyncChannel(std::shared_ptr<Mrpcchannel> transport);

	void saveCallee(ControllerPtr controller, MessagePtr request, MessagePtr response,
	    ClosurePtr done = nullptr);

	// 只投递 RPC Fiber，方法本身不等待网络响应。
	void CallMethod(const MethodDescriptor *method, RpcController *controller,
	    const Message *request, Message *response, Closure *done) override;

	// 挂起当前 Fiber；RPC 完成后由原 Scheduler 恢复，不阻塞系统线程。
	void wait();

	bool finished() const;

  private:
	void fail(const std::string &reason, RpcController *fallbackController);
	void postCompletion();

	std::shared_ptr<Mrpcchannel> m_channel;
	ControllerPtr m_controller;
	MessagePtr m_request;
	MessagePtr m_response;
	ClosurePtr m_done;

	moczkrin::Scheduler *m_scheduler = nullptr;
	std::shared_ptr<moczkrin::Fiber> m_callerFiber;
	int m_callerThread = -1;

	std::atomic<bool> m_prepared{false};
	std::atomic<bool> m_started{false};
	std::atomic<bool> m_waiting{false};
	std::atomic<bool> m_finished{false};
};


} // namespace mraft
