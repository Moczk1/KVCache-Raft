//
// Created by swx on 23-12-28.
//
#include "common/Option.h"
#include "kvserver/KvServer.h"
#include <boost/program_options.hpp>
#include <iostream>
#include <random>
#include <unistd.h>

void ShowArgsHelp();

int main(int argc, char **argv)
{
	namespace po = boost::program_options;
	//////////////////////////////////读取命令参数：节点数量、写入raft节点节点信息到哪个文件
	if (argc < 2)
	{
		ShowArgsHelp();
		exit(EXIT_FAILURE);
	}

	mraft::Option opt;

	po::options_description desc("Allowed options");

	desc.add_options()("help,h", "show help message")("log_file,l",
	    po::value<std::string>(&opt.logFile),
	    "log file path, default = log.txt")("raftfile,r",
	    po::value<std::string>(&opt.m_raftFileName), "raftfile file path")(
	    "snapshot,s", po::value(&opt.m_snapshotFileName), "snapshot file path")(
	    "nodeNum,n", po::value(&opt.nodeNum), "node number")(
	    "config,f", po::value(&opt.configFileName), "config file");

	po::variables_map vm;

	po::store(po::parse_command_line(argc, argv, desc), vm);

	po::notify(vm);

	if (vm.count("help"))
	{
		std::cout << desc << std::endl;
		return 0;
	}

	int nodeNum = 0;
	std::random_device rd;
	std::mt19937 gen(rd());
	std::uniform_int_distribution<> dis(10000, 29999);
	unsigned short startPort = dis(gen);

	std::ofstream file(opt.configFileName, std::ios::out | std::ios::app);
	file.close();
	file = std::ofstream(opt.configFileName, std::ios::out | std::ios::trunc);
	if (file.is_open())
	{
		file.close();
		std::cout << opt.configFileName << " 已清空" << std::endl;
	}
	else
	{
		std::cout << "无法打开 " << opt.configFileName << std::endl;
		exit(EXIT_FAILURE);
	}
	for (int i = 0; i < opt.nodeNum; i++)
	{
		short port = startPort + static_cast<short>(i);
		std::cout << "start to create raftkv node:" << i << "    port:" << port
		          << " pid:" << getpid() << std::endl;
		pid_t pid = fork(); // 创建新进程
		if (pid == 0)
		{
			// 如果是子进程
			// 子进程的代码

			auto kvServer =
			    new mraft::KvServer(i, 500, opt.configFileName, port, opt);
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
