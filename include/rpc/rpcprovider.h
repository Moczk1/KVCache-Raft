#pragma once
#include "ioscheduler.h"
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <memory>
#include <muduo/base/Timestamp.h>
#include <muduo/net/Buffer.h>
#include <muduo/net/Callbacks.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/TcpServer.h>
#include <string>
#include <unordered_map>

namespace mraft
{
// rpc 服务 server 的包装类
class RpcProvider
{

  private:
	struct RpcCallContext
	{
		uint64_t requestId;
		std::string serviceName;
		std::string methodName;
		std::shared_ptr<google::protobuf::Message> request;
		std::shared_ptr<google::protobuf::Message> response;
	};


	enum class VarintReadResult
	{
		Ok,
		NeedMore,
		BadFormat,
	};


	constexpr inline static uint32_t kMaxFrameSize = 16 * 1024 * 1024; // 16MB

  public:
	// 发布 rpc 方法的函数接口
	void NotifyService(google::protobuf::Service *service);

	// 启动 rpc 服务，开始提供远程网络调用服务
	void Run(int nodeIndex, short port);

  private:
	// 事件循环
	muduo::net::EventLoop m_eventLoop;
	std::shared_ptr<muduo::net::TcpServer> m_muduo_server;

	std::unique_ptr<moczkrin::IOManager> m_iomanager;

	struct ServiceInfo
	{
		google::protobuf::Service *m_service; // service 对象
		// 方法的存储结构
		std::unordered_map<std::string,
		    const google::protobuf::MethodDescriptor *>
		    m_methodMap; // 保存方法
	};
	// 存储注册成功的服务对象和其服务方法的所有信息
	std::unordered_map<std::string, ServiceInfo> m_serviceMap;

	// 新的 socket 连接回调
	void OnConnection(const muduo::net::TcpConnectionPtr &);

	// 已建立连接的用户读写事件回调
	void OnMessage(const muduo::net::TcpConnectionPtr &, muduo::net::Buffer *, muduo::Timestamp);

	// Closure 的回调操作，用于序列化 rpc 的响应和网络发送。
	void SendRpcResponse(
	    muduo::net::TcpConnectionPtr , const std::shared_ptr<RpcCallContext> ctx);



	VarintReadResult TryReadVarint32(
	    const char *data, size_t len, uint32_t *value, size_t *varintLen)
	{
		uint32_t result = 0;

		for (size_t i = 0; i < len && i < 5; ++i)
		{
			uint8_t byte = static_cast<uint8_t>(data[i]);
			result |= static_cast<uint32_t>(byte & 0x7f) << (7 * i);

			if ((byte & 0x80) == 0)
			{
				*value = result;
				*varintLen = i + 1;
				return VarintReadResult::Ok;
			}
		}

		if (len < 5)
		{
			return VarintReadResult::NeedMore;
		}

		return VarintReadResult::BadFormat;
	}

  public:
	~RpcProvider();
};

} // namespace mraft