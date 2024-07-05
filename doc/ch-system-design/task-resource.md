(task-resource)=
# 任务执行与资源划分

:::{note}

本教程已出版为《Flink 原理与实践》，感兴趣的读者请在各大电商平台购买！

<a href="https://item.jd.com/13154364.html"> ![](https://img.shields.io/badge/JD-%E8%B4%AD%E4%B9%B0%E9%93%BE%E6%8E%A5-red) </a>


:::

## 再谈逻辑视图到物理执行图

了解了 Flink 的分布式架构和核心组件，这里我们从更细粒度上来介绍从逻辑视图转化为物理执行图过程，该过程可以分成四层：`StreamGraph` -> `JobGraph` -> `ExecutionGraph` -> 物理执行图。我们根据 {numref}`fig-data-flow-graph` 来大致了解一些这些图的功能。

* `StreamGraph`：根据用户编写的代码生成的最初的图，用来表示一个 Flink 流处理作业的拓扑结构。在 `StreamGraph` 中，节点 `StreamNode` 就是算子。 

* `JobGraph`：`JobGraph` 是提交给 JobManager 的数据结构。`StreamGraph` 经过优化后生成了 `JobGraph`，主要的优化为，将多个符合条件的节点链接在一起作为一个 `JobVertex` 节点，这样可以减少数据交换所需要的传输开销。这个链接的过程叫做算子链（Operator Chain），我们会在下一小节继续介绍算子链。`JobVertex` 经过算子链后，会包含一到多个算子，它的输出是 `IntermediateDataSet`，这是经过算子处理产生的数据集。

* `ExecutionGraph`：JobManager 将 `JobGraph` 转化为 `ExecutionGraph`。`ExecutionGraph` 是 `JobGraph` 的并行化版本：假如某个 `JobVertex` 的并行度是 2，那么它将被划分为 2 个 `ExecutionVertex`，`ExecutionVertex` 表示一个算子子任务，它监控着单个子任务的执行情况。每个 `ExecutionVertex` 会输出一个 `IntermediateResultPartition`，这是单个子任务的输出，再经过 `ExecutionEdge` 输出到下游节点。`ExecutionJobVertex` 是这些并行子任务的合集，它监控着整个算子的运行情况。`ExecutionGraph` 是调度层非常核心的数据结构。

* 物理执行图：JobManager 根据 `ExecutionGraph` 对作业进行调度后，在各个 TaskManager 上部署具体的任务，物理执行图并不是一个具体的数据结构。

```{figure} ./img/graph.svg
---
name: fig-data-flow-graph
width: 80%
align: center
---
数据流图的转化过程
```

可以看到，Flink 在数据流图上可谓煞费苦心，仅各类图就有四种之多。对于新人来说，可以不用太关心这些非常细节的底层实现，只需要了解以下几个核心概念：

* Flink 采用主从架构，Master 起着管理协调作用，TaskManager 负责物理执行，在执行过程中会发生一些数据交换、生命周期管理等事情。

* 用户调用 Flink API，构造逻辑视图，Flink 会对逻辑视图优化，并转化为并行化的物理执行图，最后被执行的是物理执行图。

## 任务、算子子任务与算子链

在构造物理执行图的过程中，Flink 会将一些算子子任务链接在一起，组成算子链。链接后以任务（Task）的形式被 TaskManager 调度执行。使用算子链是一个非常有效的优化，它可以有效降低算子子任务之间的传输开销。链接之后形成的 Task 是 TaskManager 中的一个线程。{numref}`fig-operator-chain` 展示了任务、子任务和算子链之间的关系。

```{figure} ./img/operator-chain.svg
---
name: fig-operator-chain
width: 80%
align: center
---
任务、子任务与算子链
```

例如，数据从 Source 前向传播到 FlatMap，这中间没有发生跨分区的数据交换，因此，我们完全可以将 Source、FlatMap 这两个子任务组合在一起，形成一个 Task。数据经过 `keyBy()` 发生了数据交换，数据会跨越分区，因此无法将 `keyBy()` 以及其后面的窗口聚合链接到一起。由于 WindowAggregation 的并行度是 2，Sink 的并行度为 1，数据再次发生了交换，我们不能把 WindowAggregation 和 Sink 两部分链接到一起。本章第一节中提到，Sink 的并行度被人为设置为 1，如果我们把 Sink 的并行度也设置为 2，那么是可以让这两个算子链接到一起的。

默认情况下，Flink 会尽量将更多的子任务链接在一起，这样能减少一些不必要的数据传输开销。但一个子任务有超过一个输入或发生数据交换时，链接就无法建立。两个算子能够链接到一起是有一些规则的，感兴趣的读者可以阅读 Flink 源码中 `org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator` 中的 `isChainable` 方法。`StreamingJobGraphGenerator` 类的作用是将 `StreamGraph` 转换为 `JobGraph`。

尽管将算子链接到一起会降低一些传输开销，但是也有一些情况并不需要太多链接。比如，有时候我们需要将一个非常长的算子链拆开，这样我们就可以将原来集中在一个线程中的计算拆分到多个线程中来并行计算。Flink 允许开发者手动配置是否启用算子链，或者对哪些算子使用算子链。我们也将在 9.3.1 讨论算子链的具体使用方法。

## 任务槽位与计算资源

### Task Slot

根据前文的介绍，我们已经了解到 TaskManager 负责具体的任务执行。在程序执行之前，经过优化，部分子任务被链接在一起，组成一个 Task。TaskManager 是一个 JVM 进程，在 TaskManager 中可以并行运行一到多个 Task。每个 Task 是一个线程，需要 TaskManager 为其分配相应的资源，TaskManager 使用 Task Slot 给 Task 分配资源。

在解释 Flink 的 Task Slot 的概念前，我们先回顾一下进程与线程的概念。在操作系统层面，进程（Process）是进行资源分配和调度的一个独立单位，线程（Thread）是 CPU 调度的基本单位。比如，我们常用的 Office Word 软件，在启动后就占用操作系统的一个进程。Windows 上可以使用任务管理器来查看当前活跃的进程，Linux 上可以使用 `top` 命令来查看。线程是进程的一个子集，一个线程一般专注于处理一些特定任务，不独立拥有系统资源，只拥有一些运行中必要的资源，如程序计数器。一个进程至少有一个线程，也可以有多个线程。多线程场景下，每个线程都处理一小个任务，多个线程以高并发的方式同时处理多个小任务，可以提高处理能力。

回到 Flink 的槽位分配机制上，一个 TaskManager 是一个进程，TaskManager 可以管理一至多个 Task，每个 Task 是一个线程，占用一个 Slot。每个 Slot 的资源是整个 TaskManager 资源的子集，比如 {numref}`fig-task-slot` 里的 TaskManager 下有 3 个 Slot，每个 Slot 占用 TaskManager 1/3 的内存，第一个 Slot 中的 Task 不会与第二个 Slot 中的 Task 互相争抢内存资源。

:::{note}
在分配资源时，Flink 并没有将 CPU 资源明确分配给各个槽位。
:::

```{figure} ./img/task-slot.svg
---
name: fig-task-slot
width: 80%
align: center
---
Task Slot 与 Task Manager
```

假设我们给 WordCount 程序分配两个 TaskManager，每个 TaskManager 又分配 3 个槽位，所以共有 6 个槽位。结合之前图中对这个作业的并行度设置，整个作业被划分为 5 个 Task，使用 5 个线程，这 5 个线程可以按照上图所示的方式分配到 6 个槽位中。

Flink 允许用户设置 TaskManager 中槽位的数目，这样用户就可以确定以怎样的粒度将任务做相互隔离。如果每个 TaskManager 只包含一个槽位，那么运行在该槽位内的任务将独享 JVM。如果 TaskManager 包含多个槽位，那么多个槽位内的任务可以共享 JVM 资源，比如共享 TCP 连接、心跳信息、部分数据结构等。官方建议将槽位数目设置为 TaskManager 下可用的 CPU 核心数，那么平均下来，每个槽位都能平均获得 1 个 CPU 核心。

### 槽位共享

之前的图展示了任务的一种资源分配方式，默认情况下， Flink 还提供了一种槽位共享（Slot Sharing）的优化机制，进一步优化数据传输开销，充分利用计算资源。将之前图中的任务做槽位共享优化后，结果如 {numref}`fig-slot-sharing` 所示。

```{figure} ./img/slot-sharing.svg
---
name: fig-slot-sharing
width: 80%
align: center
---
槽位共享示意图
```

开启槽位共享后，Flink 允许多个 Task 共享一个槽位。如上图中最左侧的数据流，一个作业从 Source 到 Sink 的所有子任务都可以放置在一个槽位中，这样数据交换成本更低。而且，对于一个数据流图来说，Source、FlatMap 等算子的计算量相对不大，WindowAggregation 算子的计算量比较大，计算量较大的算子子任务与计算量较小的算子子任务可以互补，腾出更多的槽位，分配给更多 Task，这样可以更好地利用资源。如果不开启槽位共享，如之前图所示，计算量小的 Source、FlatMap 算子子任务独占槽位，造成一定的资源浪费。

```{figure} ./img/slot-parallelism.svg
---
name: fig-slot-parallelism
width: 80%
align: center
---
槽位共享后，增大并行度，可以部署更多算子实例
```

最初图中的方式共占用 5 个槽位，支持槽位共享后，{numref}`fig-slot-sharing` 只占用 2 个槽位。为了充分利用空槽位，剩余的 4 个空槽位可以分配给别的作业，也可以通过修改并行度来分配给这个作业。例如，这个作业的输入数据量非常大，我们可以把并行度设为 6，更多的算子实例会将这些槽位填充，如 {numref}`fig-slot-parallelism` 所示。

综上，Flink 的一个槽位中可能运行一个算子子任务、也可以是被链接的多个子任务组成的 Task，或者是共享槽位的多个 Task，具体这个槽位上运行哪些计算由算子链和槽位共享两个优化措施决定。我们将在 9.3 节再次讨论算子链和槽位共享这两个优化选项。

并行度和槽位数目的概念可能容易让人混淆，这里再次阐明一下。用户使用 Flink 提供的 API 算子可以构建一个逻辑视图，需要将任务并行才能被物理执行。一个算子将被切分为多个子任务，每个子任务处理整个作业输入数据的一部分。如果输入数据过大，增大并行度可以让算子切分为更多的子任务，加快数据处理速度。可见，并行度是 Flink 对任务并行切分的一种描述。槽位数目是在资源设置时，对单个 TaskManager 的资源切分粒度。关于并行度、槽位数目等配置，将在 9.2.2 中详细说明。 