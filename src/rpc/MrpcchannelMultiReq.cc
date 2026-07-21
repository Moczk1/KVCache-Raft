#include "rpc/MrpcchannelMultiReq.h"
#include "ThreadPool.h"
#include "common/util.h"
#include "rpc/mrpccontroller.h"
#include "rpcheader.pb.h"
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <memory>
#include <mutex>
#include <print>
#include <utility>

namespace mraft
{
MrpcchannelMultiReq::MrpcchannelMultiReq(
    const std::string &ip, const short port, bool connectNow, int retry)
    : m_ip(ip), m_port(port), m_retry(retry),
      m_recipient(std::make_unique<moczkrin::IOManager>(1, false)),
      m_sender(std::make_unique<moczkrin::IOManager>(1, false)),
      m_worker(std::make_unique<moczkrin::IOManager>(1, false))
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
	m_recipient->scheduleLock([this] { this->recvLoop(); });
}

void MrpcchannelMultiReq::CallMethod(const MethodDescriptor *method, RpcController *controller,
    const Message *request, Message *response, Closure *done)
{
	if (m_clientFd == -1)
	{
		std::string errMsg;
		// 保证连接正常
		bool rt = newConnect(m_ip.c_str(), m_port, &errMsg);
		if (!rt)
		{
			std::print("Function:{},重连接ip:{}; port:{}失败\n", __FUNCTION__, m_ip, m_port);
			controller->SetFailed(errMsg);
			return;
		}
		else
		{
			std::print("Function:{},重连接ip:{}; port:{}成功\n", __FUNCTION__, m_ip, m_port);
		}
	}

	auto requestId = m_nextRequset_id.fetch_add(1, std::memory_order_relaxed);
	// 准备返回消息后需要的变量
	struct MrpcchannelMultiReq::PendingCall pending{};
	{
		std::lock_guard<std::mutex> lock(lifetime_mutex);
		if (m_rpc_calllifetime.find(controller) == m_rpc_calllifetime.end())
		{
			controller->SetFailed("controller not found!");
			return;
		}
		else
		{
			auto it = m_rpc_calllifetime.find(controller);
			response = it->second->response.get();
			request = it->second->request.get();
			done = it->second->done.get();

			std::unique_lock<std::mutex> lock(m_pendingMutex);

			pending.controll = it->second->controller;
			pending.done = it->second->done;
			pending.response = it->second->response;
			pending.requestId = requestId;

			m_pendings.insert({pending.requestId, pending});

			lock.unlock();

			m_rpc_calllifetime.erase(it);
		}
	}

	auto service_discriptor = method->service();
	const auto service_name = service_discriptor->name();
	const auto method_name = method->name();

	RPC::RpcRequestFrame frame{};

	frame.set_request_id(requestId);
	frame.set_method_name(method_name);
	frame.set_service_name(service_name);

	std::string req_str{};
	if (!request->SerializePartialToString(&req_str))
	{
		controller->SetFailed("request message serlize to string failed!");
		return;
	}
	frame.set_payload(req_str);

	std::string frame_str{};
	if (!frame.SerializePartialToString(&frame_str))
	{
		controller->SetFailed("frame serialize to string failed.");
		return;
	}

	std::string wireString{};
	{
		google::protobuf::io::StringOutputStream ss(&wireString);
		google::protobuf::io::CodedOutputStream cs(&ss);

		cs.WriteVarint32(static_cast<uint32_t>(frame_str.size()));
		cs.WriteString(frame_str);
	} // 网络发送的消息 string 已经写入了 wireString 变量中


	// std::print("send vote response: requestId={}, payloadSize={}\n", requestId, req_str.size());


	// 投放进 coroutine 队列
	auto task = [this, wire = std::move(wireString)]()
	{
		std::lock_guard<std::mutex> lock(m_sendMutex);
		this->sendAll(wire.data(), wire.size(), nullptr);
	};
	m_sender->scheduleLock(std::move(task), -1);
}

bool MrpcchannelMultiReq::sendAll(const char *data, size_t size, std::string *errMsg)
{
	ssize_t sent = 0;
	while (sent < size)
	{
		ssize_t n = ::send(m_clientFd, data + sent, size - sent, 0);

		if (n > 0)
		{
			sent += n;
			continue;
		}

		if (n < 0 && errno == EINTR)
		{
			continue;
		}

		*errMsg = std::format("[{}-MrpcchannelMultiReq-{}]::send failed\n", GetTime(), __func__);

		return false;
	}
	return true;
}

// bool MrpcchannelMultiReq::recv

bool MrpcchannelMultiReq::recvExact(void *buffer, size_t size)
{
	char *data = static_cast<char *>(buffer);

	size_t recived = 0;

	while (recived < size)
	{
		ssize_t n = ::recv(m_clientFd, data + recived, size - recived, 0);
		if (n > 0)
		{
			recived += static_cast<size_t>(n);
			continue;
		}

		if (n == 0)
		{
			// 对段关闭
			return false;
		}

		if (errno == EINTR)
		{
			continue;
		}

		return false;
	}
	return true;
}
bool MrpcchannelMultiReq::recvVarint32(uint32_t &value)
{
	value = 0;
	for (int shift = 0; shift < 35; shift += 7)
	{
		unsigned char byte = 0;

		if (!recvExact(&byte, 1))
		{
			return false;
		}

		value |= static_cast<uint32_t>(byte & 0x7f) << shift;

		if ((byte & 0x80) == 0)
		{
			return true;
		}
	}
	return false;
}

// iomanager function
void MrpcchannelMultiReq::recvLoop()
{
	while (!m_stopped.load(std::memory_order_acquire))
	{
		if (m_clientFd == -1)
		{
			std::string errMsg;
			auto rt = newConnect(m_ip.c_str(), m_port, &errMsg);

			int retry = m_retry;

			while (!rt && retry-- > 0)
			{
				std::cout << errMsg << std::endl;
				rt = newConnect(m_ip.c_str(), m_port, &errMsg);
			}
			if (!rt)
				return;
		}

		uint32_t frameSize = 0;

		if (!recvVarint32(frameSize))
		{
			failAllPending("connection closed while reading frame length");
			return;
		}

		if (frameSize == 0 || frameSize > kMaxFrameSize)
		{
			failAllPending("invalid response frame size");
			return;
		}

		std::string frameBody(frameSize, '\0');

		if (!recvExact(frameBody.data(), frameBody.size()))
		{
			failAllPending("connection closed while reading frame body");
			return;
		}

		RPC::RpcResponseFrame frame;

		if (!frame.ParseFromString(frameBody))
		{
			failAllPending("parse response frame failed");
			return;
		}

		completeResponse(frame);
	}
}

void MrpcchannelMultiReq::failAllPending(const std::string &info)
{
	std::print("{}\n", info);
	return;
}
void MrpcchannelMultiReq::completeResponse(const RPC::RpcResponseFrame &frame)
{
	PendingCall pending;

	{
		std::lock_guard lock(m_pendingMutex);

		// std::print("complete response: requestId={}, payloadSize={}\n", frame.request_id(),
		// frame.payload().size());

		auto it = m_pendings.find(frame.request_id());
		if (it == m_pendings.end())
		{
			// 可能是已超时并被移除的响应
			return;
		}

		pending = std::move(it->second);
		m_pendings.erase(it);
	}

	auto task = [pending = std::move(pending), payload = frame.payload()]() mutable
	{
		if (!pending.response->ParseFromString(payload))
		{
			pending.controll->SetFailed("parse RPC response payload failed");
		}

		if (pending.done)
		{
			pending.done->Run();
		}
	};

	// auto m_scheduler = moczkrin::Scheduler::GetThis();
	m_worker->scheduleLock(std::move(task), -1);
}

bool MrpcchannelMultiReq::newConnect(const char *ip, uint16_t port, std::string *errMsg)
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

void MrpcchannelMultiReq::registerCall(
    RpcController *controller, std::shared_ptr<RpcCallLifetime> ctx)
{
	{
		std::lock_guard<std::mutex> lock(lifetime_mutex);
		if (m_rpc_calllifetime.find(controller) == m_rpc_calllifetime.end())
		{
			m_rpc_calllifetime.insert({controller, ctx});
		}
		else
		{
			exit(-11);
		}
	}
}

} // namespace mraft
