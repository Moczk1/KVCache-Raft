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
