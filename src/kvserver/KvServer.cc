#include "KvServer.h"
#include "Constant.h"
#include "util.h"
#include <mutex>

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

}
} // namespace mraft