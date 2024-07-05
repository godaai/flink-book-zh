(checkpoint)=
# Checkpoint

:::{note}

本教程已出版为《Flink 原理与实践》，感兴趣的读者请在各大电商平台购买！

<a href="https://item.jd.com/13154364.html"> ![](https://img.shields.io/badge/JD-%E8%B4%AD%E4%B9%B0%E9%93%BE%E6%8E%A5-red) </a>


:::

在上一节中，我们介绍了 Flink 的状态都是基于本地的，而 Flink 又是一个部署在多节点的分布式系统，分布式系统经常出现进程被杀、节点宕机或网络中断等问题，那么本地的状态在遇到故障时如何保证不丢呢？Flink 定期保存状态数据到存储上，故障发生后从之前的备份中恢复，这个过程被称为 Checkpoint 机制。Checkpoint 为 Flink 提供了 Exactly-Once 的投递保障。本节将介绍 Flink 的 Checkpoint 机制的原理，介绍中会使用多个概念：快照（Snapshot）、分布式快照（Distributed Snapshot）、检查点（Checkpoint）等，这些概念均指的是 Flink 的 Checkpoint 机制提供的数据备份过程，读者可以将这些概念等同看待。

## Flink 分布式快照流程

首先我们来看一下一个简单的 Checkpoint 的大致流程：

1. 暂停处理新流入数据，将新数据缓存起来。
2. 将算子子任务的本地状态数据拷贝到一个远程的持久化存储上。
3. 继续处理新流入的数据，包括刚才缓存起来的数据。

Flink 是在 Chandy–Lamport 算法 [^1] 的基础上实现了一种分布式快照算法。在介绍 Flink 的快照详细流程前，我们先要了解一下检查点分界线（Checkpoint Barrier）的概念。如{numref}`fig-checkpoint-barrier` 所示，Checkpoint Barrier 被插入到数据流中，它将数据流切分成段。Flink 的 Checkpoint 逻辑是，一段新数据流入导致状态发生了变化，Flink 的算子接收到 Checpoint Barrier 后，对状态进行快照。每个 Checkpoint Barrier 有一个 ID，表示该段数据属于哪次 Checkpoint。如{numref}`fig-checkpoint-barrier` 所示，当 ID 为 n 的 Checkpoint Barrier 到达每个算子后，表示要对 n-1 和 n 之间状态更新做快照。Checkpoint Barrier 有点像 Event Time 中的 Watermark，它被插入到数据流中，但并不影响数据流原有的处理顺序。

```{figure} ./img/checkpoint-barrier.png
---
name: fig-checkpoint-barrier
width: 80%
align: center
---
Checkpoint Barrier
```

接下来，我们构建一个并行数据流图，用这个并行数据流图来演示 Flink 的分布式快照机制。这个数据流图的并行度为 2，数据流会在这些并行算子上从 Source 流动到 Sink。

首先，Flink 的检查点协调器（Checkpoint Coordinator）触发一次 Checkpoint（Trigger Checkpoint），这个请求会发送给 Source 的各个子任务。

```{figure} ./img/checkpoint-1.png
---
name: fig-checkpoint-trigger
width: 80%
align: center
---
JobManager 触发一次 Checkpoint
```

各 Source 算子子任务接收到这个 Checkpoint 请求之后，会将自己的状态写入到状态后端，生成一次快照，并且会向下游广播 Checkpoint Barrier。

```{figure} ./img/checkpoint-2.png
---
name: fig-checkpoint-source
width: 80%
align: center
---
Source 将自身状态写入状态后端，向下游发送 Checkpoint Barrier
```

Source 算子做完快照后，还会给 Checkpoint Coodinator 发送一个确认，告知自己已经做完了相应的工作。这个确认中包括了一些元数据，其中就包括刚才备份到 State Backend 的状态句柄，或者说是指向状态的指针。至此，Source 完成了一次 Checkpoint。跟 Watermark 的传播一样，一个算子子任务要把 Checkpoint Barrier 发送给所连接的所有下游子任务。

```{figure} ./img/checkpoint-3.png
---
name: fig-checkpoint-ack
width: 80%
align: center
---
Snapshot 之后发送 ACK 给 JobManager
```

对于下游算子来说，可能有多个与之相连的上游输入，我们将算子之间的边称为通道。Source 要将一个 ID 为 n 的 Checkpoint Barrier 向所有下游算子广播，这也意味着下游算子的多个输入通道里都会收到 ID 为 n 的 Checkpoint Barrier，而且不同输入通道里 Checkpoint Barrier 的流入速度不同，ID 为 n 的 Checkpoint Barrier 到达的时间不同。Checkpoint Barrier 传播的过程需要进行对齐（Barrier Alignment），我们从数据流图中截取一小部分，以下图为例，来分析 Checkpoint Barrier 是如何在算子间传播和对齐的。

```{figure} ./img/barrier-alignment.png
---
name: fig-barrier-propagation
width: 80%
align: center
---
Barrier 在算子间传播过程
```

如 {numref}`fig-barrier-propagation` 所示，对齐分为四步：

1. 算子子任务在某个输入通道中收到第一个 ID 为 n 的 Checkpoint Barrier，但是其他输入通道中 ID 为 n 的 Checkpoint Barrier 还未到达，该算子子任务开始准备进行对齐。
2. 算子子任务将第一个输入通道的数据缓存下来，同时继续处理其他输入通道的数据，这个过程被称为对齐。
3. 第二个输入通道 ID 为 n 的 Checkpoint Barrier 抵达该算子子任务，所有通道 ID 为 n 的 Checkpoint Barrier 都到达该算子子任务，该算子子任务执行快照，将状态写入 State Backend，然后将 ID 为 n 的 Checkpoint Barrier 向下游所有输出通道广播。
4. 对于这个算子子任务，快照执行结束，继续处理各个通道中新流入数据，包括刚才缓存起来的数据。

数据流图中的每个算子子任务都要完成一遍上述的对齐、快照、确认的工作，当最后所有 Sink 算子确认完成快照之后，说明 ID 为 n 的 Checkpoint 执行结束，Checkpoint Coordinator 向 State Backend 写入一些本次 Checkpoint 的元数据。

```{figure} ./img/checkpoint-4.png
---
name: fig-checkpoint-completion
width: 80%
align: center
---
Sink 算子向 JobManager 发送 ACK，一次 Checkpoint 完成
```

之所以要进行对齐，主要是为了保证一个 Flink 作业所有算子的状态是一致的，也就是说，一个 Flink 作业前前后后所有算子写入 State Backend 的状态都是基于同样的数据。

## 快照性能优化方案

前面和大家介绍了一致性快照的具体流程，这种方式保证了数据的一致性，但有一些潜在的问题：

1. 每次进行 Checkpoint 前，都需要暂停处理新流入数据，然后开始执行快照，假如状态比较大，一次快照可能长达几秒甚至几分钟。
2. Checkpoint Barrier 对齐时，必须等待所有上游通道都处理完，假如某个上游通道处理很慢，这可能造成整个数据流堵塞。

针对这些问题 Flink 已经有了一些解决方案，并且还在不断优化。

对于第一个问题，Flink 提供了异步快照（Asynchronous Snapshot）的机制。当实际执行快照时，Flink 可以立即向下广播 Checkpoint Barrier，表示自己已经执行完自己部分的快照。同时，Flink 启动一个后台线程，它创建本地状态的一份拷贝，这个线程用来将本地状态的拷贝同步到 State Backend 上，一旦数据同步完成，再给 Checkpoint Coordinator 发送确认信息。拷贝一份数据肯定占用更多内存，这时可以利用写入时复制（Copy-on-Write）的优化策略。Copy-on-Write 指：如果这份内存数据没有任何修改，那没必要生成一份拷贝，只需要有一个指向这份数据的指针，通过指针将本地数据同步到 State Backend 上；如果这份内存数据有一些更新，那再去申请额外的内存空间并维护两份数据，一份是快照时的数据，一份是更新后的数据。是否开启 Asynchronous Snapshot 是可以配置的，下一节使用不同的 State Backend 将介绍如何配置。

对于第二个问题，Flink 允许跳过对齐这一步，或者说一个算子子任务不需要等待所有上游通道的 Checkpoint Barrier，直接将 Checkpoint Barrier 广播，执行快照并继续处理后续流入数据。为了保证数据一致性，Flink 必须将那些上下游正在传输的数据也作为状态保存到快照中，一旦重启，这些元素会被重新处理一遍。这种不需要对齐的 Checkpoint 机制被称为 Unaligned Checkpoint，我们可以通过 `env.getCheckpointConfig().enableUnalignedCheckpoints();` 开启 Unaligned Checkpoint。Unaligned Checkpoint 也是支持 Exactly-Once 的。Unaligned Checkpoint 不执行 Checkpoint Barrier 对齐，因此在负载较重的场景下表现更好，但这并不意味这 Unaligned Checkpoint 就是最优方案，由于要将正在传输的数据也进行快照，状态数据会很大，磁盘负载会加重，同时更大的状态意味着重启后状态恢复的时间也更长，运维管理的难度更大。

## State Backend

前面已经分享了 Flink 的快照机制，其中 State Backend 起到了持久化存储数据的重要功能。Flink 将 State Backend 抽象成了一种插件，并提供了三种 State Backend，每种 State Backend 对数据的保存和恢复方式略有不同。接下来我们开始详细了解一下 Flink 的 State Backend。

### MemoryStateBackend

从名字中可以看出，这种 State Backend 主要基于内存，它将数据存储在 Java 的堆区。当进行分布式快照时，所有算子子任务将自己内存上的状态同步到 JobManager 的堆上。因此，一个作业的所有状态要小于 JobManager 的内存大小。这种方式显然不能存储过大的状态数据，否则将抛出 `OutOfMemoryError` 异常。这种方式只适合调试或者实验，不建议在生产环境下使用。下面的代码告知一个 Flink 作业使用内存作为 State Backend，并在参数中指定了状态的最大值，默认情况下，这个最大值是 5MB。

```java
env.setStateBackend(new MemoryStateBackend(MAX_MEM_STATE_SIZE));
```

如果不做任何配置，默认情况是使用内存作为 State Backend。

### FsStateBackend

这种方式下，数据持久化到文件系统上，文件系统包括本地磁盘、HDFS 以及包括 Amazon、阿里云在内的云存储服务。使用时，我们要提供文件系统的地址，尤其要写明前缀，比如：`file://`、`hdfs://` 或 `s3://`。此外，这种方式支持 Asynchronous Snapshot，默认情况下这个功能是开启的，可加快数据同步速度。

```java
// 使用 HDFS 作为 State Backend
env.setStateBackend(new FsStateBackend("hdfs://namenode:port/flink-checkpoints/chk-17/"));

// 使用阿里云 OSS 作为 State Backend
env.setStateBackend(new FsStateBackend("oss://<your-bucket>/<object-name>"));

// 使用 Amazon 作为 State Backend
env.setStateBackend(new FsStateBackend("s3://<your-bucket>/<endpoint>"));

// 关闭 Asynchronous Snapshot
env.setStateBackend(new FsStateBackend(checkpointPath, false));
```

Flink 的本地状态仍然在 TaskManager 的内存堆区上，直到执行快照时状态数据会写到所配置的文件系统上。因此，这种方式能够享受本地内存的快速读写访问，也能保证大容量状态作业的故障恢复能力。

### RocksDBStateBackend

这种方式下，本地状态存储在本地的 RocksDB 上。RocksDB 是一种嵌入式 Key-Value 数据库，数据实际保存在本地磁盘上。比起 `FsStateBackend` 的本地状态存储在内存中，RocksDB 利用了磁盘空间，所以可存储的本地状态更大。然而，每次从 RocksDB 中读写数据都需要进行序列化和反序列化，因此读写本地状态的成本更高。快照执行时，Flink 将存储于本地 RocksDB 的状态同步到远程的存储上，因此使用这种 State Backend 时，也要配置分布式存储的地址。Asynchronous Snapshot 在默认情况也是开启的。

此外，这种 State Backend 允许增量快照（Incremental Checkpoint），Incremental Checkpoint 的核心思想是每次快照时只对发生变化的数据增量写到分布式存储上，而不是将所有的本地状态都拷贝过去。Incremental Checkpoint 非常适合超大规模的状态，快照的耗时将明显降低，同时，它的代价是重启恢复的时间更长。默认情况下，Incremental Checkpoint 没有开启，需要我们手动开启。

```java
// 开启 Incremental Checkpoint
boolean enableIncrementalCheckpointing = true;
env.setStateBackend(new RocksDBStateBackend(checkpointPath, enableIncrementalCheckpointing));
```

相比 `FsStateBackend`，`RocksDBStateBackend` 能够支持的本地和远程状态都更大，Flink 社区已经有 TB 级的案例。

除了上述三种之外，开发者也可以自行开发 State Backend 的具体实现。

## 故障重启恢复流程

### 重启恢复基本流程

Flink 的重启恢复逻辑相对比较简单：

1. 重启应用，在集群上重新部署数据流图。
2. 从持久化存储上读取最近一次的 Checkpoint 数据，加载到各算子子任务上。
3. 继续处理新流入的数据。

这样的机制可以保证 Flink 内部状态的 Excatly-Once 一致性。至于端到端的 Exactly-Once 一致性，要根据 Source 和 Sink 的具体实现而定，我们还会在第 7 章端到端 Exactly-Once 详细讨论。当发生故障时，一部分数据有可能已经流入系统，但还未进行 Checkpoint，Source 的 Checkpoint 记录了输入的 Offset；当重启时，Flink 能把最近一次的 Checkpoint 恢复到内存中，并根据 Offset，让 Source 从该位置重新发送一遍数据，以保证数据不丢不重。像 Kafka 等消息队列是提供重发功能的，`socketTextStream` 就不具有这种功能，也意味着不能保证端到端的 Exactly-Once 投递保障。

当一个作业出现故障，进行重启时，势必会暂停一段时间，这段时间上游数据仍然继续发送过来。作业被重新拉起后，肯定需要将刚才未处理的数据消化掉。这个过程可以被理解为，一次跑步比赛，运动员不慎跌倒，爬起来重新向前追击。为了赶上当前最新进度，作业必须以更快的速度处理囤积的数据。所以，在设定资源时，我们必须留出一定的富余量，以保证重启后这段“赶进度”过程中的资源消耗。

### 三种重启策略

一般情况下，一个作业遇到一些异常情况会导致运行异常，潜在的异常情况包括：机器故障、部署环境抖动、流量激增、输入数据异常等。以输入数据异常为例，如果一个作业发生了故障重启，如果触发故障的原因没有根除，那么重启之后仍然会出现故障。因此，在解决根本问题之前，一个作业很可能无限次地故障重启，陷入死循环。为了避免重启死循环，Flink 提供了三种重启策略：

* 固定延迟（Fixed Delay）策略：作业每次失败后，按照设定的时间间隔进行重启尝试，重启次数不会超过某个设定值。
* 失败率（Failure Rate）策略：计算一个时间段内作业失败的次数，如果失败次数小于设定值，继续重启，否则不重启。
* 不重启（No Restart）策略：不对作业进行重启。

重启策略的前提是作业进行了 Checkpoint，如果作业未设置 Checkpoint，则会使用 No Restart 的策略。重启策略可以在 `conf/flink-conf.yaml` 中设置，所有使用这个配置文件执行的作业都将采用这样的重启策略；也可以在单个作业的代码中配置重启策略。

#### Fixed Delay

Fixed Delay 策略下，作业最多重启次数不会超过某个设定值，两次重启之间有一个可设定的延迟时间。例如，我们在 `conf/flink-conf.yaml` 中设置为：

```yaml
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 3
restart-strategy.fixed-delay.delay: 10 s
```

这表示作业最多自动重启 3 次，两次重启之间有 10 秒的延迟。超过最多重启次数后，该作业被认定为失败。两次重启之间有延迟，是考虑到一些作业与外部系统有连接，连接一般会设置超时，频繁建立连接对数据准确性和作业运行都不利。如果在程序中用代码配置，可以写为：

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 开启 Checkpoint
env.enableCheckpointing(5000L);
env.setRestartStrategy(
  RestartStrategies.fixedDelayRestart(
    3, // 尝试重启次数
    Time.of(10L, TimeUnit.SECONDS) // 两次重启之间的延迟为 10 秒
  ));
```

如果开启了 Checkpoint，但没有设置重启策略，Flink 会默认使用这个策略，最大重启次数为 `Integer.MAX_VALUE`。

#### Failure Rate

Failure Rate 策略下，在设定的时间内，重启失败次数小于设定阈值，该作业继续重启，重启失败次数超出设定阈值，该作业被最终认定为失败。两次重启之间会有一个等待的延迟。例如，我们在 `conf/flink-conf.yaml` 中设置为：

```yaml
restart-strategy: failure-rate
restart-strategy.failure-rate.max-failures-per-interval: 3
restart-strategy.failure-rate.failure-rate-interval: 5 min
restart-strategy.failure-rate.delay: 10 s
```

这表示在 5 分钟的时间内，重启次数小于 3 次时，继续重启，否则认定该作业为失败。两次重启之间的延迟为 10 秒。在程序中用代码配置，可以写为：

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 开启 Checkpoint
env.enableCheckpointing(5000L);
env.setRestartStrategy(RestartStrategies.failureRateRestart(
  3, // 5 分钟内最多重启 3 次
  Time.of(5, TimeUnit.MINUTES), 
  Time.of(10, TimeUnit.SECONDS) // 两次重启之间延迟为 10 秒
));
```

#### No Restart

No Restart 策略下，一个作业遇到异常情况后，直接被判定为失败，不进行重启尝试。在 `conf/flink-conf.yaml` 中设置为：

```yaml
restart-strategy: none
```

使用代码配置，可以写为：

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setRestartStrategy(RestartStrategies.noRestart());
```

## Checkpoint 相关配置

默认情况下，Checkpoint 机制是关闭的，需要调用 `env.enableCheckpointing(n)` 来开启，每隔 n 毫秒进行一次 Checkpoint。Checkpoint 是一种负载较重的任务，如果状态比较大，同时 n 值又比较小，那可能一次 Checkpoint 还没完成，下次 Checkpoint 已经被触发，占用太多本该用于正常数据处理的资源。增大 n 值意味着一个作业的 Checkpoint 次数更少，整个作业用于进行 Checkpoint 的资源更小，可以将更多的资源用于正常的流数据处理。同时，更大的 n 值意味着重启后，整个作业需要从更长的 Offset 开始重新处理数据。

此外，还有一些其他参数需要配置，这些参数统一封装在了 `CheckpointConfig` 里：

```java
CheckpointConfig checkpointCfg = env.getCheckpointConfig();
```

默认的 Checkpoint 配置使用了 Checkpoint Barrier 对齐功能，对齐会增加作业的负担，有一定延迟，但是可以支持 Exactly-Once 投递的。这里的 Exactly-Once 指的是除去 Source 和 Sink 外其他各算子的 Exactly-Once，关于 Exactly-Once，我们将在第七章进一步详细解释。Checkpoint Barrier 对齐能保证在重启恢复时，各算子的状态对任一条数据只处理一次。如果作业对延迟的要求很低，那么应该使用 At-Least-Once 投递，不进行对齐，但某些数据会被处理多次。

```java
// 使用 At-Least-Once
checkpointCfg.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
```

如果一次 Checkpoint 超过一定时间仍未完成，直接将其终止，以免其占用太多资源：

```java
// 超时时间 1 小时
checkpointCfg.setCheckpointTimeout(3600*1000);
```

如果两次 Checkpoint 之间的间歇时间太短，那么正常的作业可能获取的资源较少，更多的资源被用在了 Checkpoint 上。对下面这个参数进行合理配置能保证数据流的正常处理。比如，设置这个参数为 60 秒，那么前一次 Checkpoint 结束后 60 秒内不会启动新的 Checkpoint。这种模式只在整个作业最多允许 1 个 Checkpoint 时适用。

```java
// 两次 Checkpoint 的间隔为 60 秒
checkpointCfg.setMinPauseBetweenCheckpoints(60*1000);
```

默认情况下一个作业只允许 1 个 Checkpoint 执行，如果某个 Checkpoint 正在进行，另外一个 Checkpoint 被启动，新的 Checkpoint 需要挂起等待。

```java
// 最多同时进行 3 个 Checkpoint
checkpointCfg.setMaxConcurrentCheckpoints(3);
```

如果这个参数大于 1，将与前面提到的最短间隔相冲突。

Checkpoint 的初衷是用来进行故障恢复，如果作业是因为异常而失败，Flink 会保存远程存储上的数据；如果开发者自己取消了作业，远程存储上的数据都会被删除。如果开发者希望通过 Checkpoint 数据进行调试，自己取消了作业，同时希望将远程数据保存下来，需要设置为：

```java
// 作业取消后仍然保存 Checkpoint
checkpointCfg.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
```

`RETAIN_ON_CANCELLATION` 模式下，用户需要自己手动删除远程存储上的 Checkpoint 数据。

默认情况下，如果 Checkpoint 过程失败，会导致整个应用重启，我们可以关闭这个功能，这样 Checkpoint 失败不影响作业的运行。

```java
checkpointCfg.setFailOnCheckpointingErrors(false);
```

[1]:  Leslie Lamport, K. Mani Chandy: Distributed Snapshots: Determining Global States of a Distributed System. In: *ACM Transactions on Computer Systems 3*. Nr. 1, Februar 1985. 