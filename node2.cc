#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "grpcpp/grpcpp.h"
#include "raft.h"

using namespace moczkrin;

void runService(RaftService &service)
{
    service.init("127.0.0.1", "50002");
    service.addPeer("127.0.0.1", "50001");

    std::thread clientThread([&service]()
                             {
        while (true)
        {
            for (const auto &[addr, stub] : service.m_peers)
            {
                grpc::ClientContext context;
                context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));
                raftRpcProctoc::AppendEntriesArgs args;
                raftRpcProctoc::AppendEntriesReply reply;

                grpc::Status status = stub->AppendEntries(&context, args, &reply);
                if (status.ok())
                {
                    std::cout << "reply.term: " << reply.term() << std::endl;
                }
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        } });

    clientThread.detach();
    service.listening();
}

int main()
{
    RaftService service;
    runService(service);

    return 0;
}
