#include "clerk/clerk.h"
#include <chrono>
#include <ratio>
#include <thread>
int main()
{
	mraft::Clerk client;
	client.Init("test.conf");
	auto start = std::chrono::system_clock::now();
	int count = 1;
	int tmp = count;
	while (tmp--)
	{
		client.Put("x", std::to_string(tmp));
		std::this_thread::sleep_for(std::chrono::milliseconds(2000));
		std::string get1 = client.Get("x");
		std::printf("get return :{%s}\n", get1.c_str());
	}
	return 0;
}