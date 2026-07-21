| time | work | problem |
| --- | --- | --- | 
| 7.14 | Delay optimization of single-threaded environment | Pressure node election time-out crash in multithreading environment,At present, the single-thread optimization scheme under start ()' is to execute a heartbeat immediately.|

```text
========== Raft Benchmark ==========
Threads              : 1
Requests/thread      : 5000
Total operations     : 10000
Elapsed              : 3.111 s
Throughput           : 3214.21 ops/s
Average latency      : 0.3111 ms/op
Longest thread time  : 3110 ms
====================================
```


| time | work | problem|
| --- | --- | --- | 
| 7.21 | 完成 cs/c 中的多请求在途发送