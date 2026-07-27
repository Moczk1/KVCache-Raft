#include "rpc/mrpccontroller.h"

namespace mraft {
MrpcController::MrpcController() {
  m_failed = false;
  m_errText = "";
}

void MrpcController::Reset() {
  m_failed = false;
  m_errText = "";
}

bool MrpcController::Failed() const { return m_failed; }

std::string MrpcController::ErrorText() const { return m_errText; }

void MrpcController::SetFailed(const std::string &reason) {
  m_failed = true;
  m_errText = reason;
}
} // namespace mraft
