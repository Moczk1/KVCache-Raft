#include "rpc/rpcprovider.h"
#include "common/Constant.h"
#include "rpcheader.pb.h"
#include <arpa/inet.h>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message.h>
#include <google/protobuf/stubs/callback.h>
#include <memory>
#include <muduo/base/Timestamp.h>
#include <muduo/net/Buffer.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/TcpServer.h>
#include <netdb.h>
#include <netinet/in.h>
#include <print>
#include <string>
#include <sys/types.h>
#include <unistd.h>

namespace mraft
{
void RpcProvider::NotifyService(google::protobuf::Service *service)
{
	ServiceInfo service_info;
	// service 指针获取 service 的服务器描述符
	const google::protobuf::ServiceDescriptor *service_discrp = service->GetDescriptor();
	// 获取 service 的名字
	std::string service_name = service_discrp->name();
	int method_count = service_discrp->method_count();
	if (DEBUG)
	{
		std::print("service name:{}, method_count:{}\n", service_name, method_count);
	}

	for (int i = 0; i < method_count; i++)
	{
		const google::protobuf::MethodDescriptor *method_dscrp = service_discrp->method(i);
		std::string method_name = method_dscrp->name();
		service_info.m_methodMap.insert({method_name, method_dscrp});
	}
	service_info.m_service = service;
	m_serviceMap.insert({service_name, service_info});
}

// 启动 rpc 服务， 开始提供 rpc 远程网络调用服务
void RpcProvider::Run(int nodeIndex, short port)
{


	char *ipC;
	char hname[128];
	::memset(hname, 0, sizeof hname);
	struct hostent *hent;

	gethostname(hname, sizeof hname);
	hent = gethostbyname(hname);
	for (int i = 0; hent->h_addr_list[i]; i++)
	{
		ipC = inet_ntoa(*(struct in_addr *)(hent->h_addr_list[i]));
	}

	std::string ip = std::string(ipC);

	std::string node = "node" + std::to_string(nodeIndex);
	std::ofstream os;
	os.open("test.conf", std::ios::app); // 追加模式打开文件
	                                     // text.conf
	if (!os.is_open())
	{
		std::print("打开文件{}失败\n", "text.conf");
		exit(EXIT_FAILURE);
	}

	os << node + "ip=" + ip << std::endl;
	os << node + "port=" + std::to_string(port) << std::endl;
	os.close();

	muduo::net::InetAddress address(ip, port);
	m_muduo_server = std::make_shared<muduo::net::TcpServer>(&m_eventLoop, address, "RpcProvider");

	m_muduo_server->setConnectionCallback(
	    std::bind(&RpcProvider::OnConnection, this, std::placeholders::_1));
	m_muduo_server->setMessageCallback(std::bind(&RpcProvider::OnMessage, this,
	    std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
	m_muduo_server->setThreadNum(4);


	m_iomanager = std::make_unique<moczkrin::IOManager>(4, false);

	if (DEBUG)
	{
		std::print("RpcProvider start service at "
		           "ip:{} port:{}\n",
		    ip, port);
	}
	m_muduo_server->start();
	m_eventLoop.loop();
}

void RpcProvider::OnConnection(const muduo::net::TcpConnectionPtr &conn)
{
	// 如果是新连接就什么都不干，即正常的接收连接即可
	if (!conn->connected())
	{
		// 和rpc client的连接断开了
		conn->shutdown();
	}
}

void RpcProvider::OnMessage(
    const muduo::net::TcpConnectionPtr &conn, muduo::net::Buffer *buffer, muduo::Timestamp)
{

	// std::string recv_buf = buffer->retrieveAllAsString();

	// google::protobuf::io::ArrayInputStream array_input(recv_buf.data(), recv_buf.size());
	// google::protobuf::io::CodedInputStream coded_input(&array_input);

	// uint32_t header_size{};
	// coded_input.ReadVarint32(&header_size);

	// std::string rpc_req_frame_str{};
	// coded_input.ReadString(&rpc_req_frame_str, header_size);



	while (true)
	{
		uint32_t frameSize = 0;
		size_t varintLen = 0;

		auto result =
		    TryReadVarint32(buffer->peek(), buffer->readableBytes(), &frameSize, &varintLen);

		if (result == VarintReadResult::NeedMore)
		{
			return;
		}

		if (result == VarintReadResult::BadFormat)
		{
			std::print("bad rpc frame length varint\n");
			conn->shutdown();
			return;
		}

		if (frameSize == 0 || frameSize > kMaxFrameSize)
		{
			std::print("invalid rpc frame size:{}\n", frameSize);
			conn->shutdown();
			return;
		}

		if (buffer->readableBytes() < varintLen + frameSize)
		{
			return;
		}

		buffer->retrieve(varintLen);
		std::string rpc_req_frame_str = buffer->retrieveAsString(frameSize);

		RPC::RpcRequestFrame recv_msg_fmt;
		std::string method_name{};
		std::string service_name{};
		std::string args_str{};
		uint64_t requestId = 0;

		if (recv_msg_fmt.ParseFromString(rpc_req_frame_str))
		{
			requestId = recv_msg_fmt.request_id();
			method_name = recv_msg_fmt.method_name();
			service_name = recv_msg_fmt.service_name();
			args_str = recv_msg_fmt.payload();
		}
		else
		{
			std::print("RpcRequestFrame parse error, frame size:{}\n", rpc_req_frame_str.size());
			conn->shutdown();
			return;
		}

		auto it = m_serviceMap.find(service_name);
		if (it == m_serviceMap.end())
		{
			std::print("service name:{} is not exist!\n", service_name);
			for (const auto &[name, info] : m_serviceMap)
			{
				std::cout << name << std::endl;
			}
			continue;
		}

		auto mit = it->second.m_methodMap.find(method_name);
		if (mit == it->second.m_methodMap.end())
		{
			std::cout << service_name << ":" << method_name << " is not exist" << std::endl;
			continue;
		}

		auto service = it->second.m_service;
		auto method = mit->second;

		std::shared_ptr<google::protobuf::Message> request(
		    service->GetRequestPrototype(method).New());

		if (!request->ParseFromString(args_str))
		{
			std::print("request parse error, service:{}, method:{}\n", service_name, method_name);
			continue;
		}

		std::shared_ptr<google::protobuf::Message> response(
		    service->GetResponsePrototype(method).New());



		std::shared_ptr<RpcCallContext> ctx_p = std::make_shared<RpcCallContext>(
		    RpcCallContext{requestId, service_name, method_name, request, response});


		google::protobuf::Closure *done = google::protobuf::NewCallback<RpcProvider,
		    ::muduo::net::TcpConnectionPtr, const std::shared_ptr<RpcCallContext>>(
		    this, &RpcProvider::SendRpcResponse, conn, ctx_p);

		auto task = [serv = service, method, ctx_p, done]()
		{ serv->CallMethod(method, nullptr, ctx_p->request.get(), ctx_p->response.get(), done); };


		m_iomanager->scheduleLock(task);

		// service->CallMethod(method, nullptr, request.get(), response.get(), done);
	}
}

// Closure 回调
// 用于序列化 rpc 的响应结果和消息发送
void RpcProvider::SendRpcResponse(
     muduo::net::TcpConnectionPtr conn, const std::shared_ptr<RpcCallContext> ctx)
{
	std::string responsePayload;

	if (!ctx->response->SerializeToString(&responsePayload))
	{
		std::print("serialize response payload "
		           "failed\n");
		return;
	}

	RPC::RpcResponseFrame responseFrame;
	responseFrame.set_request_id(ctx->requestId);
	responseFrame.set_payload(responsePayload);

	std::string frameBody;

	if (!responseFrame.SerializeToString(&frameBody))
	{
		std::print("[rpc-provider][serialize-failed] requestId={} service={} method={} type={}\n",
		    ctx->requestId, ctx->serviceName, ctx->methodName, ctx->response->GetTypeName());

		return;
	}

	std::string wireFrame;

	{
		google::protobuf::io::StringOutputStream output(&wireFrame);
		google::protobuf::io::CodedOutputStream coded(&output);

		coded.WriteVarint32(static_cast<uint32_t>(frameBody.size()));
		coded.WriteString(frameBody);
	}
	conn->send(wireFrame);
}

RpcProvider::~RpcProvider()
{
	std::print("~RpcProvider():ip:port:({})\n", this->m_muduo_server->ipPort());
	m_eventLoop.quit();
}

} // namespace mraft
