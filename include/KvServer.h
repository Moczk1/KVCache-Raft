

#include "ApplyMsg.h"
#include "LockQueue.h"
#include "kvServerRPC.pb.h"
#include "raft.h"
#include "skipList.h"
#include "util.h"
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/access.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>

namespace mraft {

class KvServer : public raftKVRpcProctoc::kvServerRpc {

private:
  const std::string OK = "OK";
  const std::string ErrNoKey = "ErrNoKey";
  const std::string ErrWrongLeader = "ErrWrongLeader";

  std::mutex m_mtx;
  int m_id;

  std::shared_ptr<raft> m_raftNode;

  // kvServer和raft节点的通信管道
  std::shared_ptr<LockQueue<ApplyMsg>> applyChan;

  // snapshot if log grows this big
  int m_maxRaftState;

  std::string m_serializedKVDate;

  SkipList<std::string, std::string> m_skipList;

  std::unordered_map<std::string, std::string> m_kvDB;

  std::unordered_map<int, LockQueue<Op> *> waitApplyCh;

  std::unordered_map<std::string, int> m_last_RequestId;

  int m_lastSnapShotRaftLogIndex;

public:
  KvServer() = delete;
  KvServer(int id, int maxraftstate, std::string nodeInforFileName, short port);

  void StartKVServer();

  void DprintfKVDB();

  void ExecuteAppendOpOnKVDB(Op op);
  void ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist);
  void ExecutePutOpOnKVDB(Op op);

  void Get(const raftKVRpcProctoc::GetArgs *args,
           raftKVRpcProctoc::GetReply *reply);

  void GetCommandFromRaft(ApplyMsg message);

  bool ifRequestDuplicate(std::string ClientId, int RequestId);

  // clerk 使用RPC远程调用
  void PutAppend(const raftKVRpcProctoc::PutAppendArgs *args,
                 raftKVRpcProctoc::PutAppendReply *reply);

  // 一直等待raft传来的 applyCh
  void ReadRaftApplyCommandLoop();

  void ReadSnapShotToInstall(std::string snapshot);

  bool SendMessageToWaitChan(const Op &op, int raftIndex);

  // 检查是否需要制作快照，需要的话就向raft之下制作快照
  void IfNeedToSendSnapShotCommand(int rafIndex, int proportion);

  void GetSnapSHotFromRaft(ApplyMsg message);

  std::string MakeSnapShot();

public:
  void PutAppend(google::protobuf::RpcController *controller,
                 const ::raftKVRpcProctoc::PutAppendArgs *request,
                 ::raftKVRpcProctoc::PutAppendReply *response,
                 ::google::protobuf::Closure *done) override;
  void Get(google::protobuf::RpcController *controller,
           const ::raftKVRpcProctoc::GetArgs *request,
           ::raftKVRpcProctoc::GetReply *response,
           ::google::protobuf::Closure *done) override;

private:
  friend class boost::serialization::access;

  template <class A> void serialize(A &ar, const unsigned int version) {
    ar & m_serializedKVDate;
    ar & m_last_RequestId;
  }

  std::string getSnapShotDate() {
    m_serializedKVDate = m_skipList.dump_file();
    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << *this;
    m_serializedKVDate.clear();
    return ss.str();
  }

  void parseFromString(const std::string &str) {
    std::stringstream ss(str);
    boost::archive::text_iarchive ia(ss);

    ia >> *this;
    m_skipList.load_file(m_serializedKVDate);
    m_serializedKVDate.clear();
  }
};
} // namespace mraft