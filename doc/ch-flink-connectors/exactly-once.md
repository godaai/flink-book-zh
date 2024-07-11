(sec-exactly-once)=
# Flink 端到端的 Exactly-Once 保障

## 故障恢复与一致性保障

在流处理系统中，确保每条数据只被处理一次（Exactly-Once）是一种理想情况。然而，现实中的系统经常因各种意外因素发生故障，如流量激增、网络抖动等。Flink 通过重启作业、读取 Checkpoint 数据、恢复状态和重新执行计算来处理这些故障。

Checkpoint 和故障恢复过程保证了内部状态的一致性，但可能导致数据重发。如 {numref}`fig-data-redundancy-issues` 所示，假设最近一次 Checkpoint 的时间戳是 3，系统在时间戳 10 处发生故障。在 3 到 10 之间处理的数据（如时间戳 5 和 8 的数据）需要重新处理。

Flink 的 Checkpoint 过程保证了作业内部的数据一致性，主要通过备份以下两类数据：
1. 作业中每个算子的状态。
2. 输入数据的偏移量 Offset。

```{figure} ./img/data-redundancy-issues.png
---
name: fig-data-redundancy-issues
width: 80%
align: center
---
Checkpoint 和故障恢复过程会有数据重发问题
```

数据重发类似于观看直播比赛的重播（Replay），但这可能导致时间戳 3 至 10 之间的数据被重发，从而引发 At-Least-Once 问题。为了实现端到端的 Exactly-Once 保障，需要依赖 Source 的重发功能和 Sink 的幂等写或事务写。

## 幂等写

幂等写（Idempotent Write）是指多次向系统写入数据只产生一次结果影响。例如，向 HashMap 插入同一个 (Key, Value) 二元组，只有第一次插入会改变 HashMap，后续插入不会改变结果。

Key-Value 数据库如 Cassandra、HBase 和 Redis 常作为 Sink 来实现端到端的 Exactly-Once 保障。但幂等写要求 (Key, Value) 必须是确定性计算的。例如，如果 Key 是 `name + curTimestamp`，每次重发时生成的 Key 不同，导致多次结果。如果 Key 是 `name + eventTimestamp`，则即使重发，Key 也是确定的。

Key-Value 数据库作为 Sink 可能遇到时间闪回问题。例如，重启后，之前提交的数据可能被错误地认为是新的操作，导致数据不一致。只有当所有数据重发完成后，数据才恢复一致性。

## 事务写

事务（Transaction）是数据库系统解决的核心问题。Flink 借鉴了数据库中的事务处理技术，结合 Checkpoint 机制来保证 Sink 只对外部输出产生一次影响。

Flink 的事务写（Transaction Write）是指，Flink 先将待输出的数据保存，暂时不提交到外部系统；等到 Checkpoint 结束，所有算子的数据一致时，再将之前保存的数据提交到外部系统。如 {numref}`fig-transactional-write` 所示，使用事务写可以避免时间戳 5 的数据多次产生输出并提交到外部系统。

```{figure} ./img/transactional-write.png
---
name: fig-transactional-write
width: 80%
align: center
---
Flink 的事务写
```

Flink 提供了两种事务写实现方式：预写日志（Write-Ahead-Log，WAL）和两阶段提交（Two-Phase-Commit，2PC）。这两种方式的主要区别在于：WAL 方式使用 Operator State 缓存待输出的数据；如果外部系统支持事务（如 Kafka），可以使用 2PC 方式，待输出数据被缓存在外部系统。

事务写能提供端到端的 Exactly-Once 保障，但牺牲了延迟，因为输出数据不再实时写入外部系统，而是分批次提交。开发者需要权衡不同需求。