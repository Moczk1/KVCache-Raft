2. 性能指标（Performance）

衡量系统在正常运行和高负载下的吞吐与延迟。

    吞吐量（Throughput）：

        写入吞吐量（TPS/QPS）： 每秒成功提交并应用到状态机的写请求数。

        读取吞吐量： 每秒处理的读请求数（需区分是走 Leader 强读，还是 Follower Read/Lease Read）。

    延迟（Latency）：

        写延迟： 从客户端发出写请求，到 Raft 集群多数派达成一致并返回响应的时间（包含 50/90/99/99.9 分位延迟）。

        读延迟： 不同读取模式下的响应时间。

    并发承载力： 随着客户端并发连接数（Concurrency）的增加，吞吐量和延迟的瓶颈曲线。

3. 高可用与容错恢复（Availability & Fault Tolerance）

评估系统在异常情况下的自愈能力。通常需要结合 混沌工程（Chaos Engineering） 注入故障。

    Leader 选举耗时（Leader Election Time）： 当前 Leader 挂掉后，集群选出新 Leader 并恢复写服务所需的时间。

    网络分区恢复时间（Partition Recovery Time）： 发生网络分区（如脑裂、多数派孤立、少数派孤立）时系统的表现，以及网络恢复后集群恢复正常服务的时间。

    节点加入/退出收敛时间： 触发成员变更（Configuration Change）时，系统重新达到稳定状态的时间及在此期间的性能抖动。

    频繁扰动下的可用性： 模拟网络丢包、高延迟、进程反复重启时，系统的吞吐量和延迟下降幅度。

4. 资源消耗与运维效率（Resource & Operations）

评估长周期运行下的系统健康度和成本。

    日志压缩与快照效率（Snapshot Efficiency）： 触发 Snapshot 时的内存、CPU、磁盘 IO 峰值，以及创建快照期间是否会导致写入停顿（Stall）。

    网络带宽占用： 心跳包（Heartbeat）和日志复制（AppendEntries）带来的网络开销。尤其是当没有业务写入时，纯心跳带来的带宽消耗。

    存储放大比： WAL（预写日志）加 Snapshot 占用的实际磁盘空间与业务数据的比例。




## 当前自动化脚本

`benchmark.cc` 覆盖吞吐量、读写延迟分位数和并发承载力；`run_benchmark.sh`
负责扫描不同负载与并发度并输出 CSV。

```bash
# 集群启动后执行。第二个参数是结果文件，可省略。
./benchmark/run_benchmark.sh ./bin/test.conf ./benchmark/results.csv

# 缩短单次测试或修改扫描范围
DURATION=10 WARMUP=2 CONCURRENCIES="1 4 8" \
  WORKLOADS="write mixed" ./benchmark/run_benchmark.sh ./bin/test.conf
```

当前 Raft 接口不能查询 Leader 身份、不能动态变更成员，也不能为 RPC 注入网络故障，
因此 Leader 选举、网络分区和节点加入/退出暂不由该脚本自动判断。实现管理/故障注入接口后，
可以在同一持续负载下记录最长请求停顿和恢复时间。
