#include "ioscheduler.h"
#include "thread.h"

#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <netinet/in.h>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

namespace {
constexpr int kPort = 8080;
constexpr size_t kMaxRequestSize = 8192;

std::atomic_bool g_running{true};
std::atomic_uint64_t g_request_id{0};
int g_listen_fd = -1;

void onSignal(int) { g_running.store(false); }

bool setNonBlocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    return false;
  }
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

void closeQuietly(int fd) {
  if (fd >= 0) {
    close(fd);
  }
}

std::string makeResponse(uint64_t request_id, const std::string &request) {
  std::string first_line = "unknown";
  size_t line_end = request.find("\r\n");
  if (line_end != std::string::npos) {
    first_line = request.substr(0, line_end);
  }

  std::ostringstream body;
  body << "my_coroutine demo server\n";
  body << "request_id: " << request_id << "\n";
  body << "worker_thread_id: " << moczkrin::Thread::GetThreadId() << "\n";
  body << "runtime: Fiber + Scheduler + IOManager(epoll)\n";
  body << "request_line: " << first_line << "\n";

  std::ostringstream response;
  const std::string body_text = body.str();
  response << "HTTP/1.1 200 OK\r\n";
  response << "Content-Type: text/plain; charset=utf-8\r\n";
  response << "Content-Length: " << body_text.size() << "\r\n";
  response << "Connection: close\r\n";
  response << "\r\n";
  response << body_text;
  return response.str();
}

void registerRead(int fd, const std::shared_ptr<std::string> &request_buffer);
void registerAccept();

void registerWrite(int fd, const std::shared_ptr<std::string> &response,
                   size_t offset) {
  moczkrin::IOManager *iom = moczkrin::IOManager::GetThis();
  if (!iom) {
    closeQuietly(fd);
    return;
  }

  iom->addEvent(fd, moczkrin::IOManager::WRITE, [fd, response, offset]() {
    size_t current = offset;
    while (current < response->size()) {
      ssize_t sent =
          send(fd, response->data() + current, response->size() - current, 0);
      if (sent > 0) {
        current += static_cast<size_t>(sent);
        continue;
      }

      if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        registerWrite(fd, response, current);
        return;
      }

      closeQuietly(fd);
      return;
    }

    closeQuietly(fd);
  });
}

void handleRead(int fd, const std::shared_ptr<std::string> &request_buffer) {
  char buffer[1024];

  while (true) {
    ssize_t received = recv(fd, buffer, sizeof(buffer), 0);
    if (received > 0) {
      request_buffer->append(buffer, static_cast<size_t>(received));
      if (request_buffer->size() > kMaxRequestSize) {
        closeQuietly(fd);
        return;
      }

      if (request_buffer->find("\r\n\r\n") != std::string::npos) {
        uint64_t request_id = ++g_request_id;
        auto response = std::make_shared<std::string>(
            makeResponse(request_id, *request_buffer));
        registerWrite(fd, response, 0);
        return;
      }
      continue;
    }

    if (received == 0) {
      closeQuietly(fd);
      return;
    }

    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      registerRead(fd, request_buffer);
      return;
    }

    closeQuietly(fd);
    return;
  }
}

void registerRead(int fd, const std::shared_ptr<std::string> &request_buffer) {
  moczkrin::IOManager *iom = moczkrin::IOManager::GetThis();
  if (!iom) {
    closeQuietly(fd);
    return;
  }

  iom->addEvent(fd, moczkrin::IOManager::READ,
                [fd, request_buffer]() { handleRead(fd, request_buffer); });
}

void acceptConnections() {
  while (g_running.load()) {
    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    int client_fd = accept(
        g_listen_fd, reinterpret_cast<sockaddr *>(&client_addr), &client_len);

    if (client_fd >= 0) {
      if (!setNonBlocking(client_fd)) {
        closeQuietly(client_fd);
        continue;
      }

      auto request_buffer = std::make_shared<std::string>();
      registerRead(client_fd, request_buffer);
      continue;
    }

    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      break;
    }

    if (errno == EINTR) {
      continue;
    }

    std::cerr << "accept failed: " << std::strerror(errno) << std::endl;
    break;
  }

  if (g_running.load()) {
    registerAccept();
  }
}

void registerAccept() {
  moczkrin::IOManager *iom = moczkrin::IOManager::GetThis();
  if (!iom || g_listen_fd < 0) {
    return;
  }

  iom->addEvent(g_listen_fd, moczkrin::IOManager::READ,
                []() { acceptConnections(); });
}

int createListenSocket() {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    std::cerr << "socket failed: " << std::strerror(errno) << std::endl;
    return -1;
  }

  int reuse = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed: " << std::strerror(errno) << std::endl;
    closeQuietly(fd);
    return -1;
  }

  sockaddr_in server_addr{};
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  server_addr.sin_port = htons(kPort);

  if (bind(fd, reinterpret_cast<sockaddr *>(&server_addr),
           sizeof(server_addr)) < 0) {
    std::cerr << "bind failed: " << std::strerror(errno) << std::endl;
    closeQuietly(fd);
    return -1;
  }

  if (listen(fd, SOMAXCONN) < 0) {
    std::cerr << "listen failed: " << std::strerror(errno) << std::endl;
    closeQuietly(fd);
    return -1;
  }

  if (!setNonBlocking(fd)) {
    std::cerr << "set non-blocking failed: " << std::strerror(errno)
              << std::endl;
    closeQuietly(fd);
    return -1;
  }

  return fd;
}
} // namespace

int main() {
  std::signal(SIGINT, onSignal);
  std::signal(SIGTERM, onSignal);

  g_listen_fd = createListenSocket();
  if (g_listen_fd < 0) {
    return 1;
  }

  std::cout << "my_coroutine demo server listening on http://127.0.0.1:"
            << kPort << std::endl;
  std::cout << "try: curl http://127.0.0.1:" << kPort << "/" << std::endl;
  std::cout << "press Ctrl+C to stop" << std::endl;

  {
    moczkrin::IOManager iom(2, true, "demo_iom");
    registerAccept();

    while (g_running.load()) {
      sleep(1);
    }

    iom.cancelEvent(g_listen_fd, moczkrin::IOManager::READ);
  }

  closeQuietly(g_listen_fd);
  g_listen_fd = -1;
  std::cout << "demo server stopped" << std::endl;
  return 0;
}
