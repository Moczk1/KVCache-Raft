#pragma once
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <mutex>
#include <string>
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

  private:
	int m_clientFd;
	const std::string m_ip;
	const uint16_t m_port;
	const int m_retryCount = 3;
	std::mutex m_mtx;
	bool newConnect(const char *ip, uint16_t prot, std::string *errMsg);
	void CallMethodImpl(const MethodDescriptor *method,
	    RpcController *controller, const Message *request, Message *response);
};
} // namespace mraft
