#pragma once

#include "clerk/raftServerRpcUtil.h"
#include <arpa/inet.h>
#include <memory>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

namespace mraft
{
class Clerk
{
  private:
	std::vector<std::shared_ptr<raftServerRpcUtil>>
	    m_servers; // 保存所有raft节点的fd
	               // //todo：全部初始化为-1，表示没有连接上
	std::string m_clientId;
	int m_requestId;
	int m_recentLeaderId; // 只是有可能是领导

	std::string Uuid()
	{
		return std::to_string(rand()) + std::to_string(rand()) +
		       std::to_string(rand()) + std::to_string(rand());
	} // 用于返回随机的clientId

	//    MakeClerk  todo
	void PutAppend(std::string key, std::string value, std::string op);

  public:
	// 对外暴露的三个功能和初始化
	void Init(std::string configFileName);
	std::string Get(std::string key);

	void Put(std::string key, std::string value);
	void Append(std::string key, std::string value);

  public:
	Clerk();
};

} // namespace mraft
