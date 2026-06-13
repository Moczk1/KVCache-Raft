#include "raftRpcUtil.h"
#include "grpcpp/grpcpp.h"

#include <string>

RaftRpcUtil::RaftRpcUtil(std::string ip, short port)
{
    std::string ip_port = ip+":"+std::to_string(port);
    // 构建远程连接通道
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(ip_port, grpc::InsecureChannelCredentials());
    // 创建远程调用代理
    std::unique_ptr< raftRpc::Stub> stub = raftRpc::NewStub(channel);
}

