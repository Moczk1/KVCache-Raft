#include "clerk.h"
#include <chrono>
int main()
{
	mraft::Clerk client;
	client.Init("test.conf");
	auto start = std::chrono::system_clock::now();
	int count = 500;
	int tmp = count;
	while (tmp--)
	{
		client.Put("x", std::to_string(tmp));
		std::string get1 = client.Get("x");
		std::printf("get return :{%s}\r\n", get1.c_str());
		
		client.Put("y", std::to_string(tmp));
		get1 = client.Get("x");
		std::printf("get return :{%s}\r\n", get1.c_str());
		
		client.Put("z", std::to_string(tmp));
		get1 = client.Get("x");
		std::printf("get return :{%s}\r\n", get1.c_str());
	}
	return 0;
}