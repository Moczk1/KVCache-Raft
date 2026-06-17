#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "grpcpp/grpcpp.h"
#include "raft.h"

using namespace moczkrin;

void runService(RaftService &service, std::vector<std::pair<std::string, std::string>> &info)
{
    service.init("127.0.0.1", "50003", info);
    std::cout << __LINE__ << std::endl;
}

int main()
{
    std::vector<std::pair<std::string, std::string>> info;
    for (int i = 50001; i < 50004; i++)
    {
        std::pair<std::string, std::string> p{"127.0.0.1", std::to_string(i)};
        info.push_back(p);
    }
    RaftService service;
    runService(service, info);

    return 0;
}
