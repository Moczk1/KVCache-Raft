#pragma once
#include "raft.h"
#include "ApplyMsg.h"
#include "Constant.h"
#include "RaftRpcUtil.h"
#include "raftRPC.pb.h"
#include <algorithm>
#include <cassert>
#include <chrono>
#include <memory>
#include <mutex>
#include <print>
#include <random>
#include <ratio>
#include <thread>
#include <unistd.h>

namespace mraft {
void raft::init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me,
                std::shared_ptr<Persister> persister,
                std::shared_ptr<LockQueue<ApplyMsg>> applyCh) {
  m_peers = peers;
  m_persister = persister;

  m_id = me;

  {
    std::unique_lock<std::mutex> lock(m_mtx);
    this->applyChan = applyCh;
    m_currentTerm = 0;
    m_state = follower;
    m_commitIndex = 0;
    m_lastApplied = 0;
    m_logs.clear();
    for (int i = 0; i < m_peers.size(); i++) {
      m_matchIndex.push_back(0);
      m_nextIndex.push_back(0);
    }
    m_voteForId = -1;

    m_lastSnapshotIndex = 0;
    m_lastSnapshotTerm = 0;
    m_lastElectionTime = now();
    m_lastHearBeatTime = now();
    // readPersist(m_persister->ReadRaftState());
    if (m_lastSnapshotIndex > 0) {
      m_lastApplied = m_lastSnapshotIndex;
    }
    if (DEBUG) {
      std::print("Sever{}, term{}, lastSnapshotIncludeIndex {} , "
                 "lastSnapshotIncludeTerm {}",
                 m_id, m_currentTerm, m_lastSnapshotIndex, m_lastSnapshotTerm);
    }
  }

  std::thread t(&raft::leaderHeartBeatTricker, this);
  t.detach();
  std::thread t2(&raft::electionTimeOutTicker, this);
  t2.detach();
  //   std::thread t3(&raft::applier)
}

// void raft::leaderUpdateCommitIndex() {
//   m_commitIndex = m_lastLogIndex;

//   int indexandterm[2] = {-1, -1};
//   getLastLogIndexandTerm(indexandterm[0], indexandterm[1]);

//   for (int index = indexandterm[0]; index >= m_lastLogIndex + 1; index--) {
//     int sum = 0;
//     for (int i = 0; i < m_peers.size(); i++) {
//       if (i == m_id) {
//         sum += 1;
//         continue;
//       }
//       if (m_matchIndex[i] >= index) {
//         sum += 1;
//       }
//     }
//   }


// }

void raft::electionTimeOutTicker() {
  while (true) {
    while (m_state == leader) {
      // usleep(__useconds_t useconds)
      std::this_thread::sleep_for(std::chrono::milliseconds(HEARTBEATTIMEOUT));
    }
    std::chrono::duration<signed long int, std::milli> suitableSleepTime{};
    std::chrono::system_clock::time_point wakeTime{};

    // 上锁防止调度. 计算时间
    {
      std::unique_lock<std::mutex> lock(m_mtx);
      wakeTime = now();
      suitableSleepTime =
          []() -> auto {
        std::random_device rd;
        std::mt19937 rng(rd());
        std::uniform_int_distribution<int> dist(MIN_ELECTION_INTERVAL,
                                                MAX_ELECTION_INTERVAL);
        return std::chrono::milliseconds(dist(rng));
      }() +
                      std::chrono::duration_cast<std::chrono::milliseconds>(
                          m_lastElectionTime - wakeTime);
    } // 解锁

    // 判断距离下次 election 的时间长度； 大于1个单位则 sleep，否则继续执行
    if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() >
        1) {
      auto start = std::chrono::steady_clock::now();
      std::this_thread::sleep_for(std::chrono::milliseconds(suitableSleepTime));
      auto end = std::chrono::steady_clock::now();

      auto duration =
          std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
      // 使用ANSI控制序列将输出颜色修改为紫色
      std::cout << "\033[1;35m electionTimeOutTicker();函数设置睡眠时间为: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                       suitableSleepTime)
                       .count()
                << " 毫秒\033[0m" << std::endl;
      std::cout << "\033[1;35m electionTimeOutTicker();函数实际睡眠时间为: "
                << duration.count() << " 毫秒\033[0m" << std::endl;
    }

    if (std::chrono::duration_cast<std::chrono::milliseconds>(
            m_lastElectionTime - wakeTime)
            .count() > 0) {
      continue;
    }
    doElection();
  }
}

void raft::doElection() {
  std::unique_lock<std::mutex> lock(m_mtx);

  if (m_state == leader) {
    return;
  }

  if (m_state != leader) {
    std::print("{}:{}::\t\traft:{}选举定时器到期且不是leader，开始选举 \n",
               __FUNCTION__, __LINE__, m_id);
    // 准备工作
    {
      std::unique_lock<std::mutex> lock(m_mtx);
      m_state = candidate;
      m_currentTerm += 1;
      m_voteForId = m_id;
    }

    // 持久化
    // persist();

    // 准备发送数据
    std::shared_ptr<int> votedNum = std::make_shared<int>(1);

    // 重置 election 时间戳
    m_lastElectionTime = now();

    // 给所有的 peer 发送选举 rpc
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_id)
        continue;
      int lastLogIndex = -1;
      int lastLogTerm = -1;
      getLastLogIndexandTerm(lastLogIndex, lastLogTerm);
      auto args = std::make_shared<raftRpcProctoc::RequestVoteArgs>();
      args->set_lastlogindex(lastLogIndex);
      args->set_lastlogterm(lastLogTerm);
      args->set_candidateid(m_id);
      args->set_term(m_currentTerm);
      auto ans = std::make_shared<raftRpcProctoc::RequestVoteReply>();
      std::thread t(&raft::sendRequestVote, this, i, args, ans, votedNum);
      t.detach();
    }
  }
}

bool raft::sendRequestVote(
    int i, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
    std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply,
    std::shared_ptr<int> votedNum) {
  auto start = now();
  std::print("{}:{}::\t\trf{}] 向server{} 發送 RequestVote 開始\n",
             __FUNCTION__, __LINE__, m_id, i);
  // 发送消息
  bool status = m_peers[i]->RequestVote(args.get(), reply.get());
  auto end = now();
  std::print("{}:{}::\t\trf{}] 向server{} 發送 RequestVote 完畢，耗時:{} ms\n",
             __FUNCTION__, __LINE__, m_id, i, end - start);

  if (!status) {
    return status; // 返回连接失败标志
  }

  // 接收到消息，根据消息对自己的状态做修改
  // 上锁
  std::unique_lock<std::mutex> lock(m_mtx);

  /**
   * 根据 term 的情况有三种变化
   */
  // 1.
  if (reply->term() > m_currentTerm) // 没有成功进入 leader 状态
  {
    m_currentTerm = reply->term();
    m_state = follower;
    m_voteForId = -1; //
    // 持久化
    // persist();

    return true;
  }
  // 2.
  else if (reply->term() < m_currentTerm) { // term 事件时间具有最高优先级
    // reply 没有被请求重置到自己的term，说明对方拒绝
    return true;
  }
  assert(reply->term() == m_currentTerm); // 断言保证后续的正确性

  // 判断对方的投票情况是否真实投递给自己
  // 原因：同一个 term 下，有多个 candidate 希望成为 leader
  if (reply->votegranted() == false) // 仍然拒绝请求
    return true;

  assert(reply->votegranted() == true); // 接收请求

  // 计票
  *votedNum += 1;

  if (*votedNum >= m_peers.size() / 2 + 1) // 如果满足过半数同意->成功晋升leader
  {
    if (m_state == leader) {
      std::print("{}:{}::\t\trf{}]  term:{} 同一个term当两次领导，error\n",
                 __FUNCTION__, __LINE__, m_id, m_currentTerm);
    }

    m_state = leader;
    int info[2] = {0};
    getLastLogIndexandTerm(info[0], info[1]);
    std::print("sendRequestVote rf{}] elect success,current term:{}"
               ",lastLogIndex:{}\n",
               m_id, m_currentTerm, info[0]);
    // 修改本地保存的 远端服务器的相关缓存
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_id)
        continue;
      m_nextIndex[i] = info[0] + 1; // 远端想要的下一个 index 编号
      m_matchIndex[i] = 0;          // 每换一个领导则重置远端的 commit 号
    }

    // 启动 成为 leader 后的定时任务
    std::thread t(&raft::doHeartBeat, this);
    t.detach();

    // 持久化
    // persist();
  }
  return true;
}

// server 远端接收到 rpc 请求
void raft::RequestVote(const ::raftRpcProctoc::RequestVoteArgs *request,
                       ::raftRpcProctoc::RequestVoteReply *response) {

  std::unique_lock<std::mutex> lock(m_mtx);

  // 持久化
  //   persist();

  /** 同样对应三种情况 */
  // 1.
  if (request->term() < m_currentTerm) {
    response->set_term(m_currentTerm);
    response->set_votegranted(false);
    response->set_votestate(expired);
    return;
  }
  // 2.
  if (request->term() > m_currentTerm) {
    m_state = follower;
    m_currentTerm = request->term();
    m_voteForId = -1;
  } // 这里不返回是因为可能 req.term 更大，但是本地具有request没有的较旧的
    // log。需要后续进行比较 index & term 两个参数;
    // 进入 3 的判断流程

  // 3.
  assert(request->term() == m_currentTerm);

  int info[2] = {0, 0};
  getLastLogIndexandTerm(info[0], info[1]);

  // 只有此 term 下第一次投票 & candidate 的日志新的程度 >= 自己的日志 才会授票
  // 即 wheter == true 时才投票
  bool whether = [&]() -> bool {
    if (request->lastlogterm() > info[1] ||
        (request->lastlogterm() == info[1] &&
         request->lastlogindex() >= info[0])) {
      return true;
    } else {
      return false;
    }
  }();

  if (!whether) {
    if (request->lastlogterm() < info[1]) {

    } else { // term == term && index < m_index
    }
    response->set_term(m_currentTerm);
    response->set_votegranted(false);
    response->set_votestate(voted);
    return;
  }

  // 检查过 request 的日志确实新
  // 但需要保证此时的 term 时第一次授票，防止同term下的多次授票
  if (m_voteForId != -1 &&
      m_voteForId != request->candidateid()) { // 并非第一次
    response->set_term(m_currentTerm);
    response->set_votegranted(false);
    response->set_votestate(voted);
    return;
  } else { // 确实为第一次
    m_voteForId = request->candidateid();
    m_lastElectionTime = now(); // 重置自己的选举时间
    response->set_term(m_currentTerm);
    response->set_votestate(normal);
    response->set_votegranted(true);
    return;
  }
}

// 重写框架的 调用接口
void raft::RequestVote(google::protobuf::RpcController *controller,
                       const ::raftRpcProctoc::RequestVoteArgs *request,
                       ::raftRpcProctoc::RequestVoteReply *response,
                       ::google::protobuf::Closure *done) {
  RequestVote(request, response);
  done->Run();
}

void raft::leaderHeartBeatTricker() {
  while (true) {
    while (m_state != leader) {
      std::this_thread::sleep_for(std::chrono::milliseconds(HEARTBEATTIMEOUT));
    }

    static std::atomic<int32_t> atomicCount = 0;
    std::chrono::duration<unsigned long int, std::milli> suitableSleepTime{};
    std::chrono::system_clock::time_point wakeTime{};

    {
      std::unique_lock<std::mutex> lock(m_mtx);
      wakeTime = now();
      suitableSleepTime =
          []() -> auto {
        std::random_device rd;
        std::mt19937 rng(rd());
        std::uniform_int_distribution<int> dist(MIN_ELECTION_INTERVAL,
                                                MAX_ELECTION_INTERVAL);
        return std::chrono::milliseconds(dist(rng));
      }() +
                      std::chrono::duration_cast<std::chrono::milliseconds>(
                          m_lastElectionTime - wakeTime);
    }

    if (std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime)
            .count() > 1) {
      std::cout << atomicCount
                << "\033[1;35m leaderHearBeatTicker();函数设置睡眠时间为: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                       suitableSleepTime)
                       .count()
                << " 毫秒\033[0m" << std::endl;
      // 获取当前时间点
      auto start = std::chrono::steady_clock::now();
      std::this_thread::sleep_for(
          std::chrono::duration<unsigned long int, std::milli>(
              suitableSleepTime));
      auto end = std::chrono::steady_clock::now();

      std::chrono::duration<double, std::milli> duration = end - start;

      std::cout << atomicCount
                << "\033[1;35m leaderHearBeatTicker();函数实际睡眠时间为: "
                << duration.count();
      atomicCount++;
    }

    if (std::chrono::duration_cast<std::chrono::milliseconds>(
            m_lastHearBeatTime - wakeTime)
            .count() > 1)
      continue;

    doHeartBeat();
  }
}
void raft::doHeartBeat() {
  std::unique_lock<std::mutex> lock(m_mtx);
  if (m_state != leader) // 非 leader 环境下直接退出此线程
    return;

  assert(m_state == leader);

  std::print(
      "{}:{}::\t\tLeader:{}] Leader的心跳定时器触发了且拿到mutex，开始发送AE\n",
      __FUNCTION__, __LINE__, m_id);

  // 正确返回的节点数量
  auto appedNum = std::make_shared<int>(1);

  for (int i = 0; i < m_peers.size(); i++) {
    if (i == m_id)
      continue;

    std::print("{}:{}::\tLeader: {} Leader的心跳定时器触发了 index:{}\n",
               __FUNCTION__, __LINE__, m_id, i);
    assert(m_nextIndex[i] >= 1);

    // 由于有持久化的数据，所需需要判断发送数据的来源
    if (m_nextIndex[i] <= m_lastSnapshotIndex) {
      std::thread t(&raft::leaderSendSnapShot, this, i);
      t.detach();
      continue; // 结束，暂时不处理 m_logs 的消息
    }

    assert(m_nextIndex[i] > m_lastSnapshotIndex);

    int preLogIndexandTerm[2] = {-1, -1};
    getPrevLogInfo(i, preLogIndexandTerm[0], preLogIndexandTerm[1]);

    // 构造发送 request 的结构体
    auto appendEntriesArgs =
        std::make_shared<raftRpcProctoc::AppendEntriesArgs>();
    appendEntriesArgs->set_term(m_currentTerm);
    appendEntriesArgs->set_leaderid(m_id);
    appendEntriesArgs->set_prevlogindex(preLogIndexandTerm[0]);
    appendEntriesArgs->set_prevlogterm(preLogIndexandTerm[1]);
    appendEntriesArgs->clear_entries();
    appendEntriesArgs->set_leadercommit(m_commitIndex);
    /**
                 j          preLogIndexandTerm[0]
                 |          |
      ------------------------------------
                |                        |
                snapshotindex           m_logs[size-1].index

    */
    if (preLogIndexandTerm[0] !=
        m_lastSnapshotIndex) { // 请求的数据开始不是m_logs的开始
      assert(preLogIndexandTerm[0] > m_lastSnapshotIndex);
      int startIndex = preLogIndexandTerm[0] - m_lastSnapshotIndex - 1;
      for (int j = startIndex + 1; j < m_logs.size(); j++) {
        raftRpcProctoc::LogEntry *sendEntryPtr =
            appendEntriesArgs->add_entries();
        *sendEntryPtr = m_logs[j];
      }
    } else { // 直接全部复制发送 m_logs
      for (const auto &item : m_logs) {
        raftRpcProctoc::LogEntry *sendEntryPtr =
            appendEntriesArgs->add_entries();
        *sendEntryPtr = item;
      }
    }

    assert(appendEntriesArgs->prevlogindex() +
               appendEntriesArgs->entries_size() ==
           m_lastLogIndex);

    auto appendEntriesReply =
        std::make_shared<raftRpcProctoc::AppendEntriesReply>();

    std::thread t(&raft::sendAppendEntries, this, i, appendEntriesArgs,
                  appendEntriesReply, appedNum);
    t.detach();
  }

  // 每次发送 更新心跳时间
  m_lastHearBeatTime = now();
}
bool raft::sendAppendEntries(
    int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
    std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply,
    std::shared_ptr<int> appendNum) {

  std::print("{}:{}::\t\traft{} leader 向节点{}发送AE rpc開始 ， "
             "args->entries_size():{}\n",
             __FUNCTION__, __LINE__, m_id, server, args->entries_size());

  bool status = m_peers[server]->AppendEntries(args.get(), reply.get());
  if (!status) {
    std::print("{}:{}::\t\traft{} leader 向节点{}发送AE rpc失敗\n",
               __FUNCTION__, __LINE__, m_id, server);
    return false;
  }

  std::print("{}:{}::\t\traft{} leader 向节点{}发送AE rpc成功\n", __FUNCTION__,
             __LINE__, m_id, server);

  std::unique_lock<std::mutex> lock(m_mtx);

  /** 对 reply 进行检查 */
  // 3种情况
  // 1.
  if (reply->term() > m_currentTerm) // server 的事件时间比自己新，需要赶上
  {
    m_currentTerm = reply->term();
    m_state = follower;
    m_voteForId = -1;
    return true;
  } else if (reply->term() < m_currentTerm) // 2.
  // server 的term 比自己小原则上leader应该会强制同步其他节点到自己的term上
  // 这里不做任何处理，因为leader term 仍然大于 follower term
  {
    return true;
  }

  // 3.
  assert(reply->term() == m_currentTerm);

  if (m_state != leader)
  // 短暂的瞬间发生了 leader 权限的转移 后续无需执行
  {
    return true;
  }
  // 日志同步失败
  if (!reply->success()) {
    // term 匹配的前提下，判单是 rpc 延迟导致的 term 相符
    if (reply->updatenextindex() != -100) {
      std::print("{}:{}::\t\trf{} "
                 "返回的日志term相等，但是不匹配，回缩nextIndex[]：{}\n",
                 __FUNCTION__, __LINE__, m_id, reply->updatenextindex());
      m_nextIndex[server] = reply->updatenextindex(); // 失败不更新
      // matchindex,重置nextindex，使得后续发送重新开始
    }
  } else { // success！
    *appendNum += 1;
    std::print("{}:{}::\t\t節點{}返回true,當前*appendNums{}\n", __FUNCTION__,
               __LINE__, m_id, *appendNum);

    // 对某个消息发送了多遍（心跳时就会再发送），那么一条消息会导致n次上涨
    m_matchIndex[server] = std::max(
        {m_matchIndex[server], args->prevlogindex() + args->entries_size()});
    m_nextIndex[server] = m_matchIndex[server] + 1;

    int lastLogIndexandTerm[2] = {0, 0};
    getLastLogIndexandTerm(lastLogIndexandTerm[0], lastLogIndexandTerm[1]);

    // 无论什么情况，远端需要的nextindex 都必须小于等于自己日志的index+1
    assert(m_nextIndex[server] <= lastLogIndexandTerm[0] + 1);

    if (*appendNum >= 1 + m_peers.size() / 2) {
      *appendNum = 0; // 保证幂等性

      // leader 只在有日志需要提交的前提下更新 commit index；
      if (args->entries_size() > 0) {
        // 打印日志信息
        std::print("{}:{}::\t\targs->entries(args->entries_size()-1).logterm(){"
                   "}, m_currentTerm{}",
                   __FUNCTION__, __LINE__,
                   args->entries(args->entries_size() - 1).logterm(),
                   m_currentTerm);

        if (args->entries(args->entries_size() - 1).logterm() ==
            m_currentTerm) {
          // 打印日志信息
          std::print(
              "{}:{}::\t\t當前term有log成功提交，更新leader的m_commitIndex "
              "from{} to{}",
              __FUNCTION__, __LINE__, m_commitIndex,
              args->prevlogindex() + args->entries_size());
          // 更新 commit index
          m_commitIndex = std::max(
              {m_commitIndex, args->prevlogindex() + args->entries_size()});
        }
      }

      // 保证系统运行的正确性：无论何时 commitindex <= loglastindex
      assert(m_commitIndex <= lastLogIndexandTerm[0]);
    }
  }
  return true;
}

// 远端执行 rpc 请求
void raft::AppendEntries(const ::raftRpcProctoc::AppendEntriesArgs *request,
                         ::raftRpcProctoc::AppendEntriesReply *response) {
  // 上锁
  std::unique_lock<std::mutex> lock(m_mtx);
  // 无论何时都要检查 term

  /** 3种情况 */
  // 1.
  if (request->term() < m_currentTerm) {
    response->set_success(false);
    response->set_term(m_currentTerm);
    response->set_updatenextindex(-100);
    std::print("{}:{}::\t\trf{} 拒绝了 因为Leader{}的term{}< rf{}.term{}\n",
               __FUNCTION__, __LINE__, m_id, request->leaderid(),
               request->term(), m_id, m_currentTerm);
    return; // 直接返回：无效的AE，不需要重置定时器
  }

  // 持久化
  // persist();

  // 2.
  if (request->term() > m_currentTerm) {
    // 更新身份状态
    m_state = follower;
    m_currentTerm = request->term();
    m_voteForId = -1;
    // 这里不返回， 尝试接收leader 的日志
  }

  // 3.
  assert(m_currentTerm == request->term());

  m_state = follower;
  // 接收到有效的 AE 重置选举超时计时器
  m_lastElectionTime = now();

  // 因为 rpc 请求可能在网络中阻塞；被接收的时候server 已经过去很久了
  // 比较日志的新旧程度
  /** 3种情况 */
  int lastLogIndexandTerm[2] = {-1, -1};
  getLastLogIndexandTerm(lastLogIndexandTerm[0], lastLogIndexandTerm[1]);
  // 1.
  if (request->prevlogindex() > lastLogIndexandTerm[0]) {
    // leader 的日志没有把自己没有的部分全部发过来
    // 保证完全的一致性
    response->set_term(m_currentTerm);
    response->set_success(false);
    response->set_updatenextindex(lastLogIndexandTerm[0] + 1);
    return;
  } else if (request->prevlogindex() < m_lastSnapshotTerm) // 2.
  {
    // leader 发送日志为 leader 日志的持久化完成的区域
    response->set_success(false);
    response->set_term(m_currentTerm);
    response->set_updatenextindex(m_lastSnapshotTerm + 1);
    return;
  }

  int loglastindexandterm[2] = {0, 0};
  getLastLogIndexandTerm(loglastindexandterm[0], lastLogIndexandTerm[1]);

  /**
                request->logterm()
                request->logindex()
                |
        ---------------------------------------
        |       |                              |
        snapshotindex                         lastlogindex
                index
                term
  */
  assert(request->prevlogindex() >= m_lastSnapshotIndex &&
         request->prevlogindex() <= lastLogIndexandTerm[0]);

  auto check = [&]() {
    int logtermInM_logs;
    if (request->prevlogindex() == m_lastSnapshotIndex) {
      logtermInM_logs = m_lastSnapshotTerm;
    } else {
      int index = request->prevlogindex() - m_lastSnapshotIndex - 1;
      logtermInM_logs = m_logs[index].logterm();
    }
    if (request->prevlogterm() == logtermInM_logs) {
      return true;
    } else {
      return false;
    }
  }();

  // request->logterm() == term;
  if (check) {
    // 要保证 request
    // 的内容不是因为在网络中阻塞变旧。如果这种情况接受就会导致丢失真实commit的内容。
    for (int i = 0; i < request->entries_size(); i++) {
      auto log = request->entries(i);
      // 新log （index）更大 直接添加
      if (log.logindex() > lastLogIndexandTerm[0]) {
        m_logs.push_back(log);
      } else {
        // 没有超过，需要进行匹配判断
        int v_logindex = log.logindex() - lastLogIndexandTerm[0] - 1;
        if (m_logs[v_logindex].logterm() == log.logterm() &&
            m_logs[v_logindex].command() != log.command()) {
          // 相同的 index、相同的 term、但是不同的 command 内容
          std::print(
              "{}:{}::\t\trf{}两节点logIndex{}和term{}相同，但是其command却不同"
              "{}:{}:::{}:{}！！\n",
              __FUNCTION__, __LINE__, m_id, log.logindex(), log.logterm(), m_id,
              m_logs[v_logindex].command(), request->leaderid(), log.command());
          exit(-1); // 程序出现严重逻辑问题
        }
        // 强制跟随 leader 的状态
        if (m_logs[v_logindex].logterm() != log.logterm()) {
          m_logs[v_logindex] = log;
        }
      } // if
    } // for

    getLastLogIndexandTerm(lastLogIndexandTerm[0], lastLogIndexandTerm[1]);

    // 保证逻辑正确性
    assert(lastLogIndexandTerm[0] >=
           request->prevlogindex() + request->entries_size());

    /** 下面判断 commit 参数 */
    if (request->leadercommit() > m_commitIndex) {
      m_commitIndex =
          std::min({request->leadercommit(), lastLogIndexandTerm[0]});
    }

    assert(lastLogIndexandTerm[0] >= m_commitIndex);

    response->set_term(m_commitIndex);
    response->set_success(true);
    return;
  } else {

    /**
                request->logterm()
                request->logindex()
                |
        ---------------------------------------
        |       |                              |
        snapshotindex                         lastlogindex
                index
                term
    */
    // request->logindex() = index
    // `but` request->logterm() != term

    response->set_updatenextindex(m_lastSnapshotIndex + 1);

    for (int index = request->prevlogindex(); index >= m_lastSnapshotIndex;
         index--) {
      int i;
      int term;
      if (index == m_lastLogIndex) {
        i = 0;
        term = m_lastSnapshotTerm;
      } else {
        i = index - m_lastSnapshotIndex - 1;
        term = m_logs[i].logterm();
      }

      if (term !=
          m_logs[request->prevlogindex() - m_lastSnapshotIndex - 1].logterm()) {
        response->set_updatenextindex(index + 1);
        break;
      }
    }

    response->set_success(false);
    response->set_term(m_currentTerm);
    return;
  } // if
}

void raft::AppendEntries(google::protobuf::RpcController *controller,
                         const ::raftRpcProctoc::AppendEntriesArgs *request,
                         ::raftRpcProctoc::AppendEntriesReply *response,
                         ::google::protobuf::Closure *done) {
  AppendEntries(request, response);
  done->Run();
}

// 本地发送 snapshot
void raft::leaderSendSnapShot(int i) {

  std::unique_lock<std::mutex> lock(m_mtx);

  raftRpcProctoc::InstallSnapshotRequest args;
  args.set_leaderid(m_id);
  args.set_term(m_currentTerm);
  args.set_lastsnapshotincludeindex(m_lastSnapshotIndex);
  args.set_lastsnapshotincludeindex(m_lastSnapshotTerm);
  args.set_data(m_persister->ReadSnapshot());

  raftRpcProctoc::InstallSnapshotResponse reply;
  lock.unlock();

  bool ok = m_peers[i]->InstallSnapshot(&args, &reply);

  if (!ok) {
    return;
  }

  if (m_state != leader && m_currentTerm != args.term()) {
    return;
  }

  // 中途发生 leader 的更改
  if (reply.term() > m_currentTerm) {
    m_currentTerm = reply.term();
    m_voteForId = -1;
    m_state = follower;

    // 持久化
    // persist();

    m_lastElectionTime = now();
    return;
  }

  m_matchIndex[i] = args.lastsnapshotincludeindex();
  m_nextIndex[i] = m_matchIndex[i] + 1;
}

// 远端接收 snapshot 保存到本地的 snapshot
void raft::InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest *args,
                           raftRpcProctoc::InstallSnapshotResponse *reply) {
  std::unique_lock<std::mutex> lock(m_mtx);
  //   lock.unlock();
  if (args->term() < m_currentTerm) {
    // 本地的term 大于 leader 的 term 。
    // leader 不再是 leader
    reply->set_term(m_currentTerm);
    return;
  }

  if (args->term() > m_currentTerm) {
    m_currentTerm = args->term();
    m_voteForId = -1;
    m_state = follower;

    // 持久化
    // persist();
  }

  assert(args->term() == m_currentTerm);

  m_state = follower;
  m_lastElectionTime = now();

  if (args->lastsnapshotincludeindex() <=
      m_lastSnapshotIndex) { // leader 的snapshot index 小于自己的snapshot
                             // 的index 是否需要返回正常的 reply 消息？
    return;
  }

  int lastindexandterm[2];
  getLastLogIndexandTerm(lastindexandterm[0], lastindexandterm[1]);

  // 从内存缓存的日志中 删除这些内容
  if (lastindexandterm[0] > args->lastsnapshotincludeindex()) {
    int index = args->lastsnapshotincludeindex() - m_lastSnapshotIndex - 1;
    m_logs.erase(m_logs.cbegin(), m_logs.begin() + index + 1);
  } else {
    m_logs.clear();
  }

  m_commitIndex = std::max(m_commitIndex, args->lastsnapshotincludeindex());
  m_lastApplied = std::max(m_lastApplied, args->lastsnapshotincludeindex());
  m_lastSnapshotIndex = args->lastsnapshotincludeindex();
  m_lastSnapshotTerm = args->lastsnapshotincludeterm();

  reply->set_term(m_currentTerm);

  ApplyMsg msg;
  msg.SnapshotValid = true;
  msg.Snapshot = args->data();
  msg.SnapshotIndex = args->lastsnapshotincludeindex();
  msg.SnapshotTerm = args->lastsnapshotincludeterm();

  std::thread t(&raft::pushMsgToKvServer, this, msg);
  t.detach();

  // m_persister->Save(persistData(), args->data());
}

void raft::pushMsgToKvServer(ApplyMsg msg) { applyChan->Push(msg); }

void raft::InstallSnapshot(
    google::protobuf::RpcController *controller,
    const ::raftRpcProctoc::InstallSnapshotRequest *request,
    ::raftRpcProctoc::InstallSnapshotResponse *response,
    ::google::protobuf::Closure *done) {
  InstallSnapshot(request, response);
  done->Run();
}

void raft::getLastLogIndexandTerm(int &index, int &term) {
  if (m_logs.empty()) {
    index = m_lastSnapshotIndex;
    term = m_lastSnapshotTerm;
    return;
  } else {
    int len = m_logs.size();
    index = m_logs[len - 1].logindex();
    term = m_logs[len - 1].logterm();
    return;
  }
}
} // namespace mraft