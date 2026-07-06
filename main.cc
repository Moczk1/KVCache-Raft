#include <boost/program_options.hpp>
#include <iostream>

namespace po = boost::program_options;

int main(int argc, char *argv[]) {


  struct option{
      std::string log_file = "./log.txt";
      std::string output = "./output.txt";
      bool verbose = false;
  };

  option opt;

  po::options_description desc("Allowed options");

  desc.add_options()("help,h", "show help message")
  ("log_file,l", po::value<std::string>(&opt.log_file),"log_file path, default = ./log.txt")
  ("output,o", po::value<std::string>(&opt.output), "output file; default = ./output.txt")
  ("verbose,v", po::bool_switch(&opt.verbose), "verbose mode; default = false");

  po::variables_map vm;

  po::store(po::parse_command_line(argc, argv, desc), vm);

  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  std::cout << "log_file = " << opt.log_file << std::endl;
  std::cout << "output = " << opt.output << std::endl;
  std::cout << "verbose = " << opt.verbose << std::endl;
}