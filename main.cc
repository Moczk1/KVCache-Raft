#include <boost/program_options.hpp>
#include <iostream>

#include "include/Option.h"

namespace po = boost::program_options;

int main(int argc, char *argv[])
{

	// struct option{
	//     std::string log_file = "./log.txt";
	//     std::string output = "./output.txt";
	//     bool verbose = false;
	// };

	mraft::Option opt;

	po::options_description desc("Allowed options");

	desc.add_options()("help,h", "show help message")(
	    "log_file,l", po::value<std::string>(&opt.logFile), "log file path, default = log.txt")(
	    "raftfile,r", po::value<std::string>(&opt.m_raftFileName), "raftfile file path")(
	    "snapshot,s", po::value(&opt.m_snapshotFileName), "snapshot file path");

	po::variables_map vm;

	po::store(po::parse_command_line(argc, argv, desc), vm);

	po::notify(vm);

	if (vm.count("help"))
	{
		std::cout << desc << std::endl;
		return 0;
	}

	std::cout << "log_file = " << opt.logFile << std::endl;
	std::cout << "raft_file = " << opt.m_raftFileName << std::endl;
	std::cout << "snapshot_file = " << opt.m_snapshotFileName << std::endl;
}