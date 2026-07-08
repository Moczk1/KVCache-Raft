#include "KvServer.h"
#include "Constant.h"
#include "LockQueue.h"
#include "util.h"
#include <cstddef>
#include <ctime>
#include <format>
#include <mutex>
#include <print>
#include <utility>

namespace mraft {

void KvServer::DprintfKVDB() {
  if (!DEBUG) {
    return;
  }
  std::unique_lock<std::mutex> lg(m_mtx);

  DeferClass defer([this] { m_skipList.display_list(); });
}
void KvServer::ExecuteAppendOpOnKVDB(Op op) {
  std::unique_lock<std::mutex> lock(m_mtx);
  m_skipList.insert_set_element(op.Key, op.Value);

  m_last_RequestId[op.ClientId] = op.RequestId;
  lock.unlock();

  DprintfKVDB();
}

void KvServer::ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist) {
  std::unique_lock<std::mutex> lock(m_mtx);
  *value = "";
  *exist = false;

  if (m_skipList.search_element(op.Key, *value)) {
    *exist = true;
  }

  m_last_RequestId[op.ClientId] = op.RequestId;
  lock.unlock();

  if (*exist) {
  } else {
  }

  DprintfKVDB();
}

void KvServer::ExecutePutOpOnKVDB(Op op) {
  std::unique_lock<std::mutex> lock(m_mtx);
  m_skipList.insert_set_element(op.Key, op.Value);

  m_last_RequestId[op.ClientId] = op.RequestId;

  lock.unlock();

  DprintfKVDB();
}

void KvServer::Get(const raftKVRpcProctoc::GetArgs *args,
                   raftKVRpcProctoc::GetReply *reply) {
  Op op;
  op.Operation = "Get";
  op.Key = args->key();
  op.Value = "";
  op.ClientId = args->clientid();
  op.RequestId = args->requestid();

  int raftIndex = -1;
  int _ = -1;
  bool isLeader = false;

  m_raftNode->Start(op, raftIndex, _, isLeader);

  // 请求的 raftNode 不再是 leader
  if (!isLeader) {
    reply->set_err(ErrWrongLeader);
  }

  std::unique_lock<std::mutex> lock(m_mtx);

  // 每一个 raftNode 都会在本地对应一个 wait 缓存结构
  if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
    // 没找到 -> 第一次与 它建立消息连接
    waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>));
  }

  auto chForRaftIndex = waitApplyCh[raftIndex];

  lock.unlock();

  // raftNode 返回消息的存储结构
  Op raftCommitOp;

  if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
    // 超时

    int _ = -1;
    bool isLeader = false;
    m_raftNode->GetState(&_, &isLeader);

    // 请求操作是否是重复的
    if (ifRequestDuplicate(op.ClientId, op.RequestId)) {
      std::string value;
      bool exist = false;

      // 从本地的缓存查找
      ExecuteGetOpOnKVDB(op, &value, &exist);
      if (exist) { // 本地缓存存在数据
        reply->set_err(OK);
        reply->set_value(value);
      } else { // 本地缓存不存在
        reply->set_err(ErrNoKey);
        reply->set_value("");
      }
    } else { // 不是重复执行的
      reply->set_err(ErrWrongLeader);
    }
  } else { // 未超时
    if (raftCommitOp.ClientId == op.ClientId &&
        raftCommitOp.RequestId == op.RequestId) {

      std::string value;
      bool exist = false;

      ExecuteGetOpOnKVDB(op, &value, &exist);

      if (exist) {
        reply->set_err(OK);
        reply->set_value(value);
      } else {
        reply->set_err(ErrNoKey);
        reply->set_value("");
      }
    } else {
      reply->set_err(ErrWrongLeader);
    }
  } // if

  lock.lock();

  auto tmp = waitApplyCh[raftIndex];
  waitApplyCh.erase(raftIndex);
  delete tmp;
  lock.unlock();
}

void KvServer::GetCommandFromRaft(ApplyMsg message) {
  Op op;
  op.parseFromString(message.Command);

  if (DEBUG) {
    time_t now = time(nullptr);
    tm *nowtm = localtime(&now);
    std::string time_s = std::format(
        "[{}-{}-{}-{}-{}-{}] ", nowtm->tm_year + 1900, nowtm->tm_mon + 1,
        nowtm->tm_mday, nowtm->tm_hour, nowtm->tm_min, nowtm->tm_sec);
    std::string info = std::format(
        "[KvServer::GetCommandFromRaft - kvserver{}],Got Command --> "
        "Index:{},ClientId{}, RequestId{},Opreation {}, Key :{}, Value :{}",
        m_id, message.CommandIndex, op.ClientId, op.RequestId, op.Operation,
        op.Key, op.Value);
    std::print("{}{}", time_s, info);
  }

  if (message.CommandIndex <= m_lastSnapShotRaftLogIndex) {
    return;
  }

  // State Machine (KVServer solute the duplicate problem)
  // duplicate command will not be exed
  if (!ifRequestDuplicate(op.ClientId, op.RequestId)) {
    // excute command
    if (op.Operation == "Put") {
      ExecutePutOpOnKVDB(op);
    }
    // if(op.Operation == "Get")
    // {
    //   std::string value;
    //   bool exist = false;
    //   ExecuteGetOpOnKVDB(op, &value,&exist);
    // }
    if (op.Operation == "Append") {
      ExecuteAppendOpOnKVDB(op);
    }

    if (m_maxRaftState != -1) {
      IfNeedToSendSnapShotCommand(message.CommandIndex, 9);
    }
    SendMessageToWaitChan(op, message.CommandIndex);
  }
}
bool KvServer::ifRequestDuplicate(std::string ClientId, int RequestId) {
  std::unique_lock<std::mutex> lock(m_mtx);

  if (m_last_RequestId.find(ClientId) == m_last_RequestId.end()) {
    return false;
  }

  return RequestId <= m_last_RequestId[ClientId];
}

void KvServer::PutAppend(const raftKVRpcProctoc::PutAppendArgs *args,
                         raftKVRpcProctoc::PutAppendReply *reply) {
  Op op;
  op.Operation = args->op();
  op.Key = args->key();
  op.Value = args->value();
  op.ClientId = args->clientid();
  op.RequestId = args->requestid();

  int raftIndex = -1;
  int _ = -1;
  bool isLeader = false;

  m_raftNode->Start(op, raftIndex, _, isLeader);

  if (!isLeader) {
    if (DEBUG) {
      std::string info = std::format(
          "[func -KvServer::PutAppend -kvserver{}]From Client {} "
          "(Request {} To Server {} key {}, raftIndex {}, but "
          "not leader",
          m_id, args->clientid(), args->requestid(), m_id, op.Key, raftIndex);

      std::print("{}{}\n", getTime(), info);
    }

    reply->set_err(ErrWrongLeader);
    return;
  }

  if (DEBUG) {
    std::string info = std::format(
        "[func -KvServer::PutAppend -kvserver{}]From Client {} (Request {}) "
        "To Server {}, key {}, raftIndex {} , but "
        "not leader",
        m_id, args->clientid(), args->requestid(), m_id, op.Key, raftIndex);
    std::print("{}{}\n", getTime(), info);
  }

  std::unique_lock<std::mutex> lock(m_mtx);

  if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
    waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>));
  }

  auto chForRaftIndex = waitApplyCh[raftIndex];

  lock.unlock();

  Op raftCommitOp;

  if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
    if (DEBUG) {
      std::string info = std::format(
          "[func -KvServer::PutAppend -kvserver{}]TIMEOUT PUTAPPEND !!!! "
          "Server {} , get Command <-- Index:{} , "
          "ClientId {}, RequestId {}, Opreation {} Key :{}, Value :{}",
          m_id, m_id, raftIndex, op.ClientId, op.RequestId, op.Operation,
          op.Key, op.Value);
      std::print("{}{}\n", getTime(), info);
    }

    if (ifRequestDuplicate(op.ClientId, op.RequestId)) {
      reply->set_err(OK); // 超时了,但因为是重复的请求，返回ok
    } else {
      reply->set_err(ErrWrongLeader); // 这里返回这个的目的让clerk重新尝试
    }
  } else {
    if (DEBUG) {
      std::string info = std::format(
          "[func -KvServer::PutAppend "
          "-kvserver{}]WaitChanGetRaftApplyMessage<--Server {} , get Command "
          "<-- Index:{} , "
          "ClientId {}, RequestId {}, Opreation {}, Key :{}, Value :{}",
          m_id, m_id, raftIndex, op.ClientId, op.RequestId, op.Operation,
          op.Key, op.Value);
      std::print("{}{}\n", getTime(), info);
    }

    if (raftCommitOp.ClientId == op.ClientId &&
        op.RequestId == raftCommitOp.RequestId) {
      reply->set_err(OK);
    } else {
      reply->set_err(ErrWrongLeader);
    }
  }

  lock.lock();
  auto tmp = waitApplyCh[raftIndex];
  waitApplyCh.erase(raftIndex);
  delete tmp;
  lock.unlock();
}

std::string KvServer::getTime() {
  std::string time_s = "";
  if (DEBUG) {
    time_t now = time(nullptr);
    tm *nowtm = localtime(&now);
    time_s = std::format("[{}-{}-{}-{}-{}-{}] ", nowtm->tm_year + 1900,
                         nowtm->tm_mon + 1, nowtm->tm_mday, nowtm->tm_hour,
                         nowtm->tm_min, nowtm->tm_sec);
    return time_s;
  }
  return "";
}
} // namespace mraft