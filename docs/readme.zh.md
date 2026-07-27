| time | work | problem |
| --- | --- | --- | 
| 7.14 |单线程环境的延迟优化|压力节点选举超时崩溃在多线程环境下，目前start()'下的单线程优化方案是立即执行一个心跳。|

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
| 7.21 | 完成 cs/c 中的多请求在途发送 | 服务端尚修改为多请求接受方式；tcp 字节流黏包问题导致 s 端不稳定，c 发送请求后会使得消息解析出错，leader 节点的变更；吞吐量下降 1/2 |

``` text
========== Raft Benchmark ==========
Threads              : 1
Requests/thread      : 5000
Total operations     : 10000
Elapsed              : 6.167 s
Throughput           : 1621.44 ops/s
Average latency      : 0.6167 ms/op
Longest thread time  : 6167 ms
====================================
```


| time | work | problem| 
| --- | --- | --- | 
| 7.23 | 完成 cs/s 中的多请求发送 | c/s  | clerk多线程请求的情况下会出现卡死：clerk的某个线程出现不能接受消息 |
```text
========== Raft Benchmark ==========
Threads              : 1
Requests/thread      : 1000
Total operations     : 2000
Elapsed              : 3.503 s
Throughput           : 570.88 ops/s
Average latency      : 1.7517 ms/op
Longest thread time  : 3503 ms
====================================

```

| time | work | problem| 
| --- | --- | --- | 
| 7.24 | 完成 cs/s 中的多请求发送 | c/s clerk正常返回，server 在高并发下出现follower 的错误退出 |
``` text
========== Raft Benchmark ==========
Threads              : 16
Requests/thread      : 50
Total operations     : 1600
Elapsed              : 3.621 s
Throughput           : 441.83 ops/s
Average latency      : 2.2633 ms/op
Longest thread time  : 3619 ms
====================================
```



| time | work | problem| 
| --- | --- | --- | 
| 7.24 | 修复上述问题 |
```text
========== Raft Benchmark ==========
Threads              : 100
Requests/thread      : 100
Total operations     : 20000
Elapsed              : 35.165 s
Throughput           : 568.75 ops/s
Average latency      : 1.7583 ms/op
Longest thread time  : 35159 ms
====================================
```


| time | work | problem| 
| --- | --- | --- | 
| 7.24 | 对比同步刷盘和异步刷盘的效率 暂时不考虑 logs 的持久化 |

```text
========== Raft MS_SYNC Benchmark ==========
Threads              : 100
Requests/thread      : 100
Total operations     : 20000
Elapsed              : 82.756 s
Throughput           : 241.67 ops/s
Average latency      : 4.1378 ms/op
Longest thread time  : 82750 ms
====================================

========== Raft MS_ASYNC Benchmark ==========
Threads              : 100
Requests/thread      : 100
Total operations     : 20000
Elapsed              : 18.029 s
Throughput           : 1109.30 ops/s
Average latency      : 0.9015 ms/op
Longest thread time  : 18026 ms
====================================
```


| time | work | problem| 
| --- | --- | --- | 
| 7.26 | 考虑 logs 的持久化 |
```text
========== Raft Benchmark ==========
Threads              : 100
Requests/thread      : 100
Total operations     : 20000
Elapsed              : 97.212 s
Throughput           : 205.74 ops/s
Average latency      : 4.8606 ms/op
Longest thread time  : 97209 ms
====================================
```
```text
========== Raft Benchmark ==========
Threads              : 100
Requests/thread      : 100
Total operations     : 20000
Elapsed              : 3.744 s
Throughput           : 5342.25 ops/s
Average latency      : 0.1872 ms/op
Longest thread time  : 3741 ms
====================================
```




```text
========== Raft Benchmark ==========
Threads              : 100
Requests/thread      : 100
Total operations     : 10000
Elapsed              : 30.144 s
Throughput           : 331.74 ops/s
Average latency      : 3.0144 ms/op
Longest thread time  : 30142 ms
====================================
```
