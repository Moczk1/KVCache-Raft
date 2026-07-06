#pragma once
#include <string>

namespace mraft {
class Option {
public:
  std::string m_raftFileName = "raftfile.txt";

  std::string m_snapshotFileName = "snapshot.txt";

  std::string logFile = "log.txt";
};
} // namespace mraft