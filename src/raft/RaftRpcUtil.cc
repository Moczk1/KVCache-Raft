#include "raft/RaftRpcUtil.h"
#include "rpc/mrpcchannel.h"
#include "rpc/mrpccontroller.h"
#include <memory>

namespace mraft {



	
// 下面三个方法内部调用 stub 的 raft rpc 方法.
bool RaftRpcUtil::AppendEntries(raftRpcProctoc::AppendEntriesArgs *args,
                                raftRpcProctoc::AppendEntriesReply *response) {
  MrpcController controller;
  m_stub->AppendEntries(&controller, args, response, nullptr);
  return !controller.Failed();
}
bool RaftRpcUtil::InstallSnapshot(
    raftRpcProctoc::InstallSnapshotRequest *args,
    raftRpcProctoc::InstallSnapshotResponse *response) {
  MrpcController controller;
  m_stub->InstallSnapshot(&controller, args, response, nullptr);
  return !controller.Failed();
}
bool RaftRpcUtil::RequestVote(raftRpcProctoc::RequestVoteArgs *args,
                              raftRpcProctoc::RequestVoteReply *response) {
  MrpcController controller;
  m_stub->RequestVote(&controller, args, response, nullptr);
  return !controller.Failed();
}

RaftRpcUtil::RaftRpcUtil(std::string ip, short port) {
  //   m_stub = new raftRpcProctoc::raftRpc_Stub(new Mrpcchannel(ip, port,
  //   true));
  m_stub = std::make_unique<raftRpcProctoc::raftRpc_Stub>(new Mrpcchannel(ip, port, true));
}
// RaftRpcUtil::~RaftRpcUtil() { delete m_stub; }
} // namespace mraft