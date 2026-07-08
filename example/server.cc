//
// Created by swx on 23-12-28.
//
#include "raft.h"
#include <KvServer.h>
#include <boost/program_options.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <iostream>
#include <random>
#include <unistd.h>

void ShowArgsHelp();

int main(int argc, char **argv)
{
	using namespace mraft;
	namespace po = boost::program_options;
	//////////////////////////////////读取命令参数：节点数量、写入raft节点节点信息到哪个文件
	if (argc < 2)
	{
		ShowArgsHelp();
		exit(EXIT_FAILURE);
	}

	Option op;

	po::options_description desc("raft node options");

	desc.add_options()("help,h", "show help message")(
	    "nodeNum,n", po::value(&op.nodeNum), "raft node numbers")(
	    "configFile,f", po::value(&op.configFileName), "config file name");

	po::variables_map vm;

	po::store(po::parse_command_line(argc, argv, desc), vm);

	po::notify(vm);

	if (vm.count("help"))
	{
		std::cout << desc << std::endl;
		return 0;
	}

	int c = 0;
	std::random_device rd;
	std::mt19937 gen(rd());
	std::uniform_int_distribution<> dis(10000, 29999);
	unsigned short startPort = dis(gen);

	std::ofstream file(op.configFileName, std::ios::out | std::ios::app);
	file.close();
	file = std::ofstream(op.configFileName, std::ios::out | std::ios::trunc);
	if (file.is_open())
	{
		file.close();
		std::cout << op.configFileName << " 已清空" << std::endl;
	}
	else
	{
		std::cout << "无法打开 " << op.configFileName << std::endl;
		exit(EXIT_FAILURE);
	}
	for (int i = 0; i < op.nodeNum; i++)
	{
		short port = startPort + static_cast<short>(i);
		std::cout << "start to create raftkv node:" << i << "    port:" << port
		          << " pid:" << getpid() << std::endl;
		pid_t pid = fork(); // 创建新进程
		if (pid == 0)
		{
			// 如果是子进程
			// 子进程的代码

			auto kvServer = new KvServer(i, 500, op.configFileName, port, op);
			pause(); // 子进程进入等待状态，不会执行 return 语句
		}
		else if (pid > 0)
		{
			// 如果是父进程
			// 父进程的代码
			sleep(1);
		}
		else
		{
			// 如果创建进程失败
			std::cerr << "Failed to create child process." << std::endl;
			exit(EXIT_FAILURE);
		}
	}
	pause();
	return 0;
}

void ShowArgsHelp()
{
	std::cout << "format: command -n <nodeNum> -f <configFileName>"
	          << std::endl;
}
