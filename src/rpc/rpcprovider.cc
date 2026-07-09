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
	const google::protobuf::ServiceDescriptor *service_discrp =
	    service->GetDescriptor();
	// 获取 service 的名字
	std::string service_name = service_discrp->name();
	int method_count = service_discrp->method_count();
	if (DEBUG)
	{
		std::print(
		    "service name:{}, method_count:{}\n", service_name, method_count);
	}

	for (int i = 0; i < method_count; i++)
	{
		const google::protobuf::MethodDescriptor *method_dscrp =
		    service_discrp->method(i);
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
	os.open("test.conf", std::ios::app); // 追加模式打开文件 text.conf
	if (!os.is_open())
	{
		std::print("打开文件{}失败\n", "text.conf");
		exit(EXIT_FAILURE);
	}

	os << node + "ip=" + ip << std::endl;
	os << node + "port=" + std::to_string(port) << std::endl;
	os.close();

	muduo::net::InetAddress address(ip, port);
	m_muduo_server = std::make_shared<muduo::net::TcpServer>(
	    &m_eventLoop, address, "RpcProvider");

	m_muduo_server->setConnectionCallback(
	    std::bind(&RpcProvider::OnConnection, this, std::placeholders::_1));
	m_muduo_server->setMessageCallback(std::bind(&RpcProvider::OnMessage, this,
	    std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
	m_muduo_server->setThreadNum(4);

	if (DEBUG)
	{
		std::print("RpcProvider start service at ip:{} port:{}\n", ip, port);
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

void RpcProvider::OnMessage(const muduo::net::TcpConnectionPtr &conn,
    muduo::net::Buffer *buffer, muduo::Timestamp)
{

	std::string recv_buf = buffer->retrieveAllAsString();

	google::protobuf::io::ArrayInputStream array_input(
	    recv_buf.data(), recv_buf.size());
	google::protobuf::io::CodedInputStream coded_input(&array_input);
	// 读取变长存储的 rpc_header 的长度
	uint32_t header_size{};
	coded_input.ReadVarint32(&header_size);

	// 根据header_size读取数据头的原始字符流，反序列化数据，得到rpc请求的详细信息
	std::string rpc_header_str; // rpc header 接收空间
	RPC::RpcHeader rpcHeader;
	std::string service_name;
	std::string method_name;
	uint32_t args_size{};

	// 设置读取大小限制
	google::protobuf::io::CodedInputStream::Limit msg_limit =
	    coded_input.PushLimit(header_size);
	// 读取 rpc header 的长度
	coded_input.ReadString(&rpc_header_str, header_size);

	// 取消读取大小限制
	coded_input.PopLimit(msg_limit);

	if (rpcHeader.ParseFromString(rpc_header_str))
	{
		// 反序列化成功
		service_name = rpcHeader.service_name();
		method_name = rpcHeader.method_name();
		args_size = rpcHeader.args_size();
	}
	else
	{
		std::print("rpc_header_str:{} parse error!", rpc_header_str);
		return;
	}

	std::string args_str;
	bool read_args_success = coded_input.ReadString(&args_str, args_size);

	if (!read_args_success)
	{
		return;
	}

	// if (DEBUG)
	// {
	// 	std::print("================================================\n"
	// 	           "rpc_header_str:{}\n "
	// 	           "service_name:{}\n"
	// 	           "method_name:{}\n"
	// 	           "args_str:{}\n",
	// 	    rpc_header_str, service_name, method_name, args_size);
	// }

	// 获取真实的 service 和 method 的描述符
	auto it = m_serviceMap.find(service_name);
	if (it == m_serviceMap.end()) // 没有找到
	{
		std::print("服务service name:{}, is not exist!\n", service_name);
		std::print("当前服务列表为\n");
		for (const auto &[name, info] : m_serviceMap)
		{
			std::cout << name << std::endl;
		}
		return;
	}
	auto mit = it->second.m_methodMap.find(method_name);
	if (mit == it->second.m_methodMap.end())
	{
		std::cout << service_name << ":" << method_name << "is not exist"
		          << std::endl;
		return;
	}

	auto service = it->second.m_service;
	auto method = mit->second;

	// 生成 rpc 方法调用的请求 request 和 响应 response
	google::protobuf::Message *request =
	    service->GetRequestPrototype(method).New();
	if (!request->ParseFromString(args_str))
	{
		std::print("request parse error, content:{}\n", args_str);
		return;
	}
	google::protobuf::Message *response =
	    service->GetResponsePrototype(method).New();

	// 设置服务端执行完毕的回调函数
	google::protobuf::Closure *done = google::protobuf::NewCallback<RpcProvider,
	    const ::muduo::net::TcpConnectionPtr &, google::protobuf::Message *>(
	    this, &RpcProvider::SendRpcResponse, conn, response);

	// 真正调用方法
	service->CallMethod(method, nullptr, request, response, done);
}

// Closure 回调
// 用于序列化 rpc 的响应结果和消息发送
void RpcProvider::SendRpcResponse(const muduo::net::TcpConnectionPtr &conn,
    google::protobuf::Message *response)
{
	std::string response_str;
	if (response->SerializeToString(&response_str))
	{
		conn->send(response_str);
	}
	else
	{
		std::print("serialize response_str error!\n");
	}
}

RpcProvider::~RpcProvider()
{
	std::print("~RpcProvider():ip:port:({})\n", this->m_muduo_server->ipPort());
	m_eventLoop.quit();
}

} // namespace mraft