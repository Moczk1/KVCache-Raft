#include "clerk/clerk.h"
#include <chrono>
#include <print>
int main()
{
	mraft::Clerk client;
	client.Init("test.conf");
	int count = 100;
	int tmp = count;
	std::chrono::steady_clock::time_point start =
	    std::chrono::steady_clock::now();
	std::chrono::steady_clock::time_point end = start;

	std::chrono::milliseconds duratin_total =
	    std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

	while (tmp--)
	{
		// start = std::chrono::steady_clock::now();
		client.Put("x", std::to_string(tmp));
		// end = std::chrono::steady_clock::now();

		// start = std::chrono::steady_clock::now();
		std::string get1 = client.Get("x");
		// start = std::chrono::steady_clock::now();
		std::printf("get return :{%s}\n", get1.c_str());
	}
	end = std::chrono::steady_clock::now();
	duratin_total =
	    std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
	;
	std::print("[info] duration total: {}\n", duratin_total);
	return 0;
}