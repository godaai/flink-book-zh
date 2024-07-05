(flink-core)=
# 架构与核心组件

:::{note}

本教程已出版为《Flink 原理与实践》，感兴趣的读者请在各大电商平台购买！

<a href="https://item.jd.com/13154364.html"> ![](https://img.shields.io/badge/JD-%E8%B4%AD%E4%B9%B0%E9%93%BE%E6%8E%A5-red) </a>


:::

为了支持分布式运行，Flink 跟其他大数据引擎一样，采用了主从（Master-Worker）架构。Flink 运行时主要包括两个组件：

• Master 是一个 Flink 作业的主进程。它起到了协调管理的作用。

• TaskManager，又被称为 Worker 或 Slave，是执行计算任务的进程。它拥有 CPU、内存等计算资源。Flink 作业需要将计算任务分发到多个 TaskManager 上并行执行。

下面将从作业执行层面来分析 Flink 各个模块如何工作。

## Flink 作业提交过程

Flink 为适应不同的基础环境（Standalone 集群、YARN、Kubernetes），在不断的迭代开发过程中已经逐渐形成了一个兼容性很强的架构。不同的基础环境对计算资源的管理方式略有不同，不过都大同小异，{numref}`fig-standalone-arch` 以 Standalone 集群为例，分析作业的分布式执行流程。Standalone 模式指 Flink 独占该集群，集群上无其他任务。

```{figure} ./img/standalone-arch.svg
---
name: fig-standalone-arch
width: 80%
align: center
---
Standalone 模式下，Flink 作业提交流程
```

在一个作业提交前，Master 和 TaskManager 等进程需要先被启动。我们可以在 Flink 主目录中执行脚本来启动这些进程：`bin/start-cluster.sh`。Master 和 TaskManager 被启动后，TaskManager 需要将自己注册给 Master 中的 ResourceManager。这个初始化和资源注册过程发生在单个作业提交前，我们称之为第 0 步。

接下来我们根据上图，逐步分析一个 Flink 作业如何被提交：

1. 用户编写应用程序代码，并通过 Flink 客户端（Client）提交作业。程序一般为 Java 或 Scala 语言，调用 Flink API，构建逻辑视角数据流图。代码和相关配置文件被编译打包，被提交到 Master 的 Dispatcher，形成一个应用作业（Application）。

2. Dispatcher 接收到这个作业，启动 JobManager，这个 JobManager 会负责本次作业的各项协调工作。

3. JobManager 向 ResourceManager 申请本次作业所需资源。

4. 由于在第 0 步中 TaskManager 已经向 ResourceManager 中注册了资源，这时闲置的 TaskManager 会被反馈给 JobManager。

5. JobManager 将用户作业中的逻辑视图转化为图所示的并行化的物理执行图，将计算任务分发部署到多个 TaskManager 上。至此，一个 Flink 作业就开始执行了。

TaskManager 在执行计算任务过程中可能会与其他 TaskManager 交换数据，会使用图中的一些数据交换策略。同时，TaskManager 也会将一些任务状态信息会反馈给 JobManager，这些信息包括任务启动、运行或终止的状态，快照的元数据等。

##  Flink 核心组件

有了这个作业提交流程，我们对各组件的功能应该有了更全面的认识，接下来我们再对涉及到的各个组件进行更为详细的介绍。

### Client

用户一般使用客户端（Client）提交作业，比如 Flink 主目录下的 `bin` 目录中提供的命令行工具。Client 会对用户提交的 Flink 程序进行预处理，并把作业提交到 Flink 集群上。Client 提交作业时需要配置一些必要的参数，比如使用 Standalone 集群还是 YARN 集群等。整个作业被打成了 Jar 包，DataStream API 被转换成了 `JobGraph`，`JobGraph` 是一种类似逻辑视图。

### Dispatcher

Dispatcher 可以接收多个作业，每接收一个作业，Dispatcher 都会为这个作业分配一个 JobManager。Dispatcher 对外提供一个 REST 式的接口，以 HTTP 的形式来对外提供服务。

### JobManager

JobManager 是单个 Flink 作业的协调者，一个作业会有一个 JobManager 来负责。JobManager 会将 Client 提交的 JobGraph 转化为 ExceutionGraph，ExecutionGraph 是类似并行的物理执行图。JobManager 会向 ResourceManager 申请必要的资源，当获取足够的资源后，JobManager 将 ExecutionGraph 以及具体的计算任务分发部署到多个 TaskManager 上。同时，JobManager 还负责管理多个 TaskManager，这包括：收集作业的状态信息，生成检查点，必要时进行故障恢复等问题。
早期，Flink Master 被命名为 JobManager，负责绝大多数 Master 进程的工作。随着迭代和开发，出现了名为 JobMaster 的组件，JobMaster 负责单个作业的执行。本书中，我们仍然使用 JobManager 的概念，表示负责单个作业的组件。一些 Flink 文档也可能使用 JobMaster 的概念，读者可以将 JobMaster 等同于 JobManager 看待。

### ResourceManager

如前文所说，Flink 现在可以部署在 Standalone、YARN 或 Kubernetes 等环境上，不同环境中对计算资源的管理模式略有不同，Flink 使用一个名为 ResourceManager 的模块来统一处理资源分配上的问题。在 Flink 中，计算资源的基本单位是 TaskManager 上的任务槽位（Task Slot，简称槽位 Slot）。ResourceManager 的职责主要是从 YARN 等资源提供方获取计算资源，当 JobManager 有计算需求时，将空闲的 Slot 分配给 JobManager。当计算任务结束时，ResourceManager 还会重新收回这些 Slot。

### TaskManager

TaskManager 是实际负责执行计算的节点。一般地，一个 Flink 作业是分布在多个 TaskManager 上执行的，单个 TaskManager 上提供一定量的 Slot。一个 TaskManager 启动后，相关 Slot 信息会被注册到 ResourceManager 中。当某个 Flink 作业提交后，ResourceManager 会将空闲的 Slot 提供给 JobManager。JobManager 获取到空闲 Slot 信息后会将具体的计算任务部署到该 Slot 之上，任务开始在这些 Slot 上执行。在执行过程，由于要进行数据交换，TaskManager 还要和其他 TaskManager 进行必要的数据通信。

总之，TaskManager 负责具体计算任务的执行，启动时它会将 Slot 资源向 ResourceManager 注册。

## Flink 组件栈

了解 Flink 的主从架构、作业提交以及核心组件等知识后，我们再从更宏观的角度来对 Flink 的组件栈分层剖析。如 {numref}`fig-flink-component` 所示，Flink 的组件栈分为四层：部署层、运行时层、API 层和上层工具。

```{figure} ./img/flink-component.svg
---
name: fig-flink-component
width: 80%
align: center
---
Flink 组件栈
```

### 部署层

Flink 支持多种部署方式，可以部署在单机 (Local)、集群 (Cluster)，以及云（Cloud）上。

* Local 模式

Local 模式有两种不同的方式，一种是单节点（SingleNode），一种是单虚拟机（SingleJVM）。

Local-SingleJVM 模式大多是开发和测试时使用的部署方式，该模式下 JobManager 和 TaskManager 都在同一个 JVM 里。

Local-SingleNode 模式下，JobManager 和 TaskManager 等所有角色都运行在一台机器上，虽然是按照分布式集群架构进行部署，但是集群的节点只有 1 个。该模式大多是在测试或者 IoT 设备上进行部署时使用。

* Cluster 模式

Flink 作业投入到生产环境下一般使用 Cluster 模式，可以是 Standalone 的独立集群，也可以是 YARN 或 Kubernetes 集群。

对于一个 Standalone 集群，我们需要在配置文件中配置好 JobManager 和 TaskManager 对应的机器，然后使用 Flink 主目录下的脚本启动一个 Standalone 集群。我们将在 9.1.1 详细介绍如何部署一个 Flink Standalone 集群。Standalone 集群上只运行 Flink 作业。除了 Flink，绝大多数企业的生产环境运行着包括 MapReduce、Spark 等各种各样的计算任务，一般都会使用 YARN 或 Kubernetes 等方式对计算资源进行管理和调度。Flink 目前已经支持了 YARN、Mesos 以及 Kubernetes，开发者提交作业的方式变得越来越简单。

* Cloud 模式

Flink 也可以部署在各大云平台上，包括 Amazon、Google 和阿里云。

### 运行时层

运行时（Runtime）层为 Flink 各类计算提供了实现。这一层读本章提到的分布式执行进行了支持。Flink Runtime 层是 Flink 最底层也是最核心的组件。

### API 层

API 层主要实现了流处理 DataStream API 和批处理 DataSet API。目前，DataStream API 针对有界和无界数据流，DataSet API 针对有界数据集。用户可以使用这两大 API 进行数据处理，包括转换（Transformation）、连接（Join）、聚合（Aggregation）、窗口（Window）以及状态（State）的计算。

### 上层工具

在 DataStream 和 DataSet 两大 API 之上，Flink 还提供了更丰富的工具，包括：

* 面向流处理的：复杂事件处理（Complex Event Process，CEP）。

* 面向批处理的：机器学习计算库（Machine Learning, ML）、图计算库（Graph Processing, Gelly）。

* 面向 SQL 用户的 Table API 和 SQL。数据被转换成了关系型数据库式的表，每个表拥有一个表模式（Schema），用户可以像操作表那样操作流式数据，例如可以使用 SELECT、JOIN、GROUP BY 等操作。

* 针对 Python 用户推出的 PyFlink，方便 Python 社区使用 Flink。目前，PyFlink 主要基于 Java 的 Table API 之上。