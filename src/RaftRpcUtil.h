#pragma once
#include "raftRPC.pb.h"
#include <memory>

namespace mraft {
class RaftRpcUtil : public std::enable_shared_from_this<RaftRpcUtil> {
private:
  std::unique_ptr<raftRpcProctoc::raftRpc_Stub> m_stub;

public:
  // 下面三个方法内部调用 stub 的 raft rpc 方法.
  bool AppendEntries(raftRpcProctoc::AppendEntriesArgs *args,
                     raftRpcProctoc::AppendEntriesReply *response);
  bool InstallSnapshot(raftRpcProctoc::InstallSnapshotRequest *args,
                       raftRpcProctoc::InstallSnapshotResponse *response);
  bool RequestVote(raftRpcProctoc::RequestVoteArgs *args,
                   raftRpcProctoc::RequestVoteReply *response);

  /**
   * @brief 创建channel -> 创建 stub -> 完成raftuitl创建
   * @param IP  远端的 ip 地址
   * @param port 远端的 port 端口号
   */
  RaftRpcUtil(std::string, short port);
  //   ~RaftRpcUtil();

private:
  RaftRpcUtil(const RaftRpcUtil &rhs) = delete;
  RaftRpcUtil(const RaftRpcUtil &&rhs) = delete;

  RaftRpcUtil &operator=(RaftRpcUtil &rhs) = delete;
  RaftRpcUtil &operator=(RaftRpcUtil &&rhs) = delete;
};
} // namespace mraft