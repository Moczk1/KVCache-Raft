#include <arpa/inet.h>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <format>
#include <functional>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <memory>
#include <netinet/in.h>
#include <print>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <utility>

#include "fiber.h"
#include "ioscheduler.h"
#include "rpc/mrpcchannel.h"
#include "rpcheader.pb.h"
#include "scheduler.h"
#include "thread.h"

namespace mraft
{
void Mrpcchannel::CallMethod(const MethodDescriptor *method,
    RpcController *controller, const Message *request, Message *response,
    Closure *done)
{
	CallMethodImpl(method, controller, request, response);
	if (done != nullptr)
	{
		done->Run();
	}
}

void Mrpcchannel::CallMethodImpl(const MethodDescriptor *method,
    RpcController *controller, const Message *request, Message *response)
{
	if (m_clientFd == -1)
	{
		std::string errMsg;
		// 保证连接正常
		bool rt = newConnect(m_ip.c_str(), m_port, &errMsg);
		if (!rt)
		{
			std::print("Function:{},重连接ip:{}; port:{}失败\n", __FUNCTION__,
			    m_ip, m_port);
			controller->SetFailed(errMsg);
			return;
		}
		else
		{
			std::print("Function:{},重连接ip:{}; port:{}成功\n", __FUNCTION__,
			    m_ip, m_port);
		}
	}
	// 获取服务器和方法名
	const google::protobuf::ServiceDescriptor *sd = method->service();
	std::string service_name = sd->name();
	std::string method_name = method->name();

	// 获取参数长度
	uint32_t args_size{};
	std::string args_str;
	if (request->SerializePartialToString(&args_str))
	{
		args_size = args_str.size();
	}
	else
	{
		controller->SetFailed("serialize request error!");
		return;
	}

	// 创建自定义 rpc 消息格式的变量
	RPC::RpcHeader rpcHeader;
	rpcHeader.set_service_name(service_name);
	rpcHeader.set_method_name(method_name);
	rpcHeader.set_args_size(args_size);

	// 序列化到 string 格式
	std::string rpc_header_str;
	if (!rpcHeader.SerializePartialToString(&rpc_header_str))
	{
		controller->SetFailed("Serialize rpc header errpr!");
		return;
	}

	// 使用protobuf的CodedOutputStream来构建发送的数据流
	std::string send_rpc_str; // 用来存储最终发送的数据
	{
		google::protobuf::io::StringOutputStream string_output(&send_rpc_str);
		google::protobuf::io::CodedOutputStream coded_output(&string_output);

		// 最开始区域 变长的 rpc_header 的长度
		coded_output.WriteVarint32(
		    static_cast<uint32_t>(rpc_header_str.size()));

		// 填写 紧跟的 rpc_header 内容
		coded_output.WriteString(rpc_header_str);
	}
	// 添加消息参数
	send_rpc_str += args_str;

	// debug 调试信息
	// if (DEBUG)
	// {
	// 	std::print("================================================\n"
	// 	           "rpc_header_str:{}\n "
	// 	           "service_name:{}\n"
	// 	           "method_name:{}\n"
	// 	           "args_str:{}\n",
	// 	    rpc_header_str, service_name, method_name, args_size);
	// }

	// 发送消息
	while (
	    -1 == ::send(m_clientFd, send_rpc_str.c_str(), send_rpc_str.size(), 0))
	{
		std::string info = std::format("send error! errno:{}", errno);
		std::print("尝试重新连接，对方ip：{}, 对方端口", m_ip, m_port);
		::close(m_clientFd);
		m_clientFd = -1;
		std::string errMsg;
		bool rt = newConnect(m_ip.c_str(), m_port, &errMsg);
		if (!rt)
		{
			controller->SetFailed(errMsg);
			return;
		}
	}

	// 接收返回结果
	char recv_buf[1024] = {0};
	::memset(recv_buf, 0, sizeof recv_buf);
	int recv_size = 0;
	if (-1 == (recv_size = ::recv(m_clientFd, recv_buf, sizeof recv_buf, 0)))
	{
		::close(m_clientFd);
		m_clientFd = -1;
		std::string errtxt = std::format("recv error! errno:{}", errno);
		controller->SetFailed(errtxt);
		return;
	}

	// 解析返回结果
	if (!response->ParseFromArray(recv_buf, recv_size))
	{
		std::string info =
		    std::format("parse error! response_str:{}", recv_size);
		controller->SetFailed(info);
		return;
	}
}

Mrpcchannel::Mrpcchannel(std::string ip, short port, bool connectNow, int retry)
    : m_clientFd(-1), m_ip(ip), m_port(port), m_retryCount(retry)
{
	if (!connectNow)
	{
		return;
	}
	std::string errMsg;
	auto rt = newConnect(ip.c_str(), m_port, &errMsg);
	while (!rt && retry-- > 0)
	{
		std::cout << errMsg << std::endl;
		rt = newConnect(m_ip.c_str(), m_port, &errMsg);
	}
}

Mrpcchannel::~Mrpcchannel()
{
	if (m_clientFd >= 0)
	{
		::close(m_clientFd);
		m_clientFd = -1;
	}
}

bool Mrpcchannel::newConnect(const char *ip, uint16_t port, std::string *errMsg)
{
	int clientFd = ::socket(AF_INET, SOCK_STREAM, 0);
	if (clientFd == -1)
	{
		*errMsg = std::format("create socket error! errno:{}", errno);
		m_clientFd = -1;
		return false;
	}
	m_clientFd = clientFd;

	struct sockaddr_in addr{};
	addr.sin_addr.s_addr = inet_addr(ip);
	addr.sin_port = htons(port);
	addr.sin_family = AF_INET;

	if (-1 == ::connect(clientFd, (struct sockaddr *)&addr, sizeof addr))
	{
		::close(clientFd);
		*errMsg = std::format("connect fail! errno:{}\n", errno);
		m_clientFd = -1;
		return false;
	}
	m_clientFd = clientFd;
	return true;
}

MrpcAsyncChannel::MrpcAsyncChannel(std::shared_ptr<Mrpcchannel> transport)
    // 必须在 RPC Fiber 内延迟建连，socket/connect hook 才能接管阻塞点。
    : m_channel(transport)
{
}

void MrpcAsyncChannel::saveCallee(ControllerPtr controller, MessagePtr request,
    MessagePtr response, ClosurePtr done)
{
	if (m_started.load(std::memory_order_acquire))
	{
		if (controller != nullptr)
		{
			controller->SetFailed(
			    "cannot replace async RPC arguments after CallMethod()");
		}
		return;
	}

	m_controller = std::move(controller);
	m_request = std::move(request);
	m_response = std::move(response);
	m_done = std::move(done);
	m_prepared.store(m_controller != nullptr && m_request != nullptr &&
	                     m_response != nullptr,
	    std::memory_order_release);
}

void MrpcAsyncChannel::CallMethod(const MethodDescriptor *method,
    RpcController *controller, const Message *request, Message *response,
    Closure *done)
{
	if (!m_prepared.load(std::memory_order_acquire))
	{
		fail("saveCallee() must be called before async CallMethod()",
		    controller);
		return;
	}

	if (controller != m_controller.get() || request != m_request.get() ||
	    response != m_response.get() ||
	    (done != nullptr && done != m_done.get()))
	{
		fail("CallMethod() arguments must match the objects saved by "
		     "saveCallee()",
		    controller);
		return;
	}

	bool expected = false;
	if (!m_started.compare_exchange_strong(
	        expected, true, std::memory_order_acq_rel))
	{
		controller->SetFailed(
		    "MrpcAsyncChannel supports only one in-flight RPC call");
		return;
	}

	m_scheduler = moczkrin::Scheduler::GetThis();
	if (m_scheduler == nullptr || moczkrin::IOManager::GetThis() == nullptr)
	{
		fail("async CallMethod() must run inside an IOManager Fiber",
		    controller);
		return;
	}

	m_callerFiber = moczkrin::Fiber::GetThis();
	m_callerThread = moczkrin::Thread::GetThreadId();

	auto self = weak_from_this().lock();
	if (self == nullptr)
	{
		fail("MrpcAsyncChannel must be owned by std::shared_ptr", controller);
		return;
	}

	m_scheduler->scheduleLock(std::function<void()>(
	    [self, method]()
	    {
		    self->m_channel->CallMethod(method, self->m_controller.get(),
		        self->m_request.get(), self->m_response.get(), nullptr);
		    self->postCompletion();
	    }));
}

void MrpcAsyncChannel::wait()
{
	if (!m_started.load(std::memory_order_acquire))
	{
		if (m_controller != nullptr)
		{
			m_controller->SetFailed("wait() called before CallMethod()");
		}
		return;
	}

	if (m_finished.load(std::memory_order_acquire))
	{
		return;
	}

	if (moczkrin::Scheduler::GetThis() != m_scheduler ||
	    moczkrin::Fiber::GetThis() != m_callerFiber)
	{
		m_controller->SetFailed(
		    "wait() must be called by the Fiber that called CallMethod()");
		return;
	}

	m_waiting.store(true, std::memory_order_release);
	while (!m_finished.load(std::memory_order_acquire))
	{
		m_callerFiber->yield();
	}
}

bool MrpcAsyncChannel::finished() const
{
	return m_finished.load(std::memory_order_acquire);
}

void MrpcAsyncChannel::fail(
    const std::string &reason, RpcController *fallbackController)
{
	RpcController *target =
	    m_controller != nullptr ? m_controller.get() : fallbackController;
	if (target != nullptr)
	{
		target->SetFailed(reason);
	}
	if (m_done != nullptr)
	{
		m_done->Run();
	}
	m_finished.store(true, std::memory_order_release);
}

void MrpcAsyncChannel::postCompletion()
{
	auto self = shared_from_this();
	m_scheduler->scheduleLock(
	    std::function<void()>(
	        [self]()
	        {
		        // 固定回到发起调用的线程，语义与 TinyRPC 回原 Reactor 一致。
		        if (self->m_done != nullptr)
		        {
			        self->m_done->Run();
		        }
		        self->m_finished.store(true, std::memory_order_release);
		        if (self->m_waiting.load(std::memory_order_acquire))
		        {
			        self->m_scheduler->scheduleLock(
			            self->m_callerFiber, self->m_callerThread);
		        }
	        }),
	    m_callerThread);
}

} // namespace mraft
