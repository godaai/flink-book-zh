(custom-source-and-sink)=
# 自定义 Source 和 Sink

在 Flink 2.0 中，Source 和 Sink 接口经历了重大的重构，旨在提供更统一和灵活的流处理与批处理接口。本文将带您逐步了解如何自定义 Source 和 Sink。

## 自定义 Source

Flink 2.0 优化了 Source 接口，使得流处理和批处理可以更加统一地进行。Source 接口引入了三个核心组件：Split、SourceReader 和 SplitEnumerator，它们共同协作以实现并行读取和数据切分。

Split 是数据源的最小处理单元，类似于数据库中的分区。例如，对于文件系统数据源，Split 可以是一个文件或文件的一部分；对于数据库，Split 可以是表中的一个分片；而对于 Kafka，Split 则是 Kafka 的 Partition。每个 Split 都有自己独立的读取进程，从而实现并行处理，提高数据读取效率。

SourceReader 是负责读取数据的组件，运行在 TaskManager 上。当数据源被切分成多个 Split 后，每个 Split 都会有一个对应的 SourceReader 来读取数据。举个例子，如果我们将一个大文件切分成三个 Split，那么就会有三个 SourceReader 同时读取这三个部分，实现并行处理。

SplitEnumerator 是在 JobManager 中运行的组件，负责管理和分配 Split。它的工作包括监控数据源的元数据、构建 Split 以及根据负载均衡策略分配 Split 给各个 SourceReader。当数据源发生变化时（比如新增文件），SplitEnumerator 会负责发现这些变化并创建新的 Split。

### 自定义 Source 示例

```java
// 自定义 Source 示例
public class MySource implements Source<String, MySplit, MySourceReader> {

    @Override
    public MySourceReader createReader(MySourceContext context, MySplit split) {
        return new MySourceReader(split);
    }

    @Override
    public SplitEnumerator<MySplit, MySourceReader> createEnumerator(
            MySourceContext context) {
        return new MySourceEnumerator();
    }

    @Override
    public SplitEnumerator<MySplit, MySourceReader> restoreEnumerator(
            MySourceContext context, MySourceEnumerator checkpoint) {
        return checkpoint;
    }
}
```

这段代码展示了如何创建一个自定义的 Source。我们可以看到，它需要实现三个核心方法：`createReader()`、`createEnumerator()` 和 `restoreEnumerator()`。

### SourceReader 实现

```java
// 自定义 SourceReader 示例
public class MySourceReader implements SourceReader<String, MySplit> {
    private final MySplit split;

    public MySourceReader(MySplit split) {
        this.split = split;
    }

    @Override
    public void pollNext(ReaderOutput<String> output) throws Exception {
        // 从 Split 中读取数据并发送到下游
        output.collect("data");
    }

    @Override
    public void close() throws Exception {
        // 清理资源，比如关闭文件句柄或网络连接
    }
}
```

SourceReader 的主要职责是从 Split 中读取数据，将数据发送到下游算子，并管理资源的生命周期。

### Split 实现

```java
// 自定义 Split 示例
public class MySplit implements Split {
    // Split 的实现，比如包含文件路径、起始位置等信息
}
```

每个 Split 都需要包含以下信息：数据源的位置信息、读取的起始位置、读取的结束位置和分区的唯一标识。

### SplitEnumerator 实现

```java
// 自定义 SplitEnumerator 示例
public class MySourceEnumerator implements SplitEnumerator<MySplit, MySourceReader> {
    @Override
    public void start() {
        // 启动时初始化 Split 的分配逻辑
    }

    @Override
    public void handleSplitRequest(int subtaskIndex, String subtaskId) {
        // 处理子任务的 Split 请求，分配 Split 给合适的 SourceReader
    }

    @Override
    public void close() {
        // 清理操作，比如释放资源
    }
}
```

SplitEnumerator 的主要职责是管理 Split 的生命周期、处理 SourceReader 的 Split 请求、实现负载均衡策略和管理 Checkpoint 的状态。

## 自定义 Sink

Sink 负责将数据写入到外部系统，比如数据库、文件系统等。Flink 2.0 的 Sink 接口设计更加灵活和强大。

### 基础 Sink 实现

```java
// 自定义 Sink 示例
public class MySink implements Sink<String> {

    @Override
    public void invoke(String value, Context context) throws Exception {
        // 写入数据到外部系统
        System.out.println("Writing value: " + value);
    }
}
```

这段代码展示了最基础的 Sink 实现，其中 `invoke()` 方法负责将数据写入到外部系统。在实际使用中，我们可能会将数据写入到数据库、文件系统、消息队列、HTTP API 等。

### 使用 Sink

```java
// 使用 Sink 的流式处理作业
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStream<String> dataStream = env.fromElements("data1", "data2", "data3");
dataStream.addSink(new MySink());
env.execute("Flink 2.0 Sink Example");
```

这段代码展示了如何将自定义的 Sink 添加到数据流中。主要步骤是创建 StreamExecutionEnvironment、创建数据流、添加 Sink 和执行作业。

## Two-Phase Commit Sink

Two-Phase Commit Sink 是 Flink 2.0 中的一个重要特性，它确保了数据写入的 Exactly-Once 语义。这种设计特别适合需要精确一次语义的场景，比如写入数据库或 Kafka。

### Two-Phase Commit 的工作流程

Two-Phase Commit 的工作流程包括四个阶段：开始事务、预提交、提交和回滚。每个阶段都有特定的职责，确保数据写入的 Exactly-Once 语义。

### Two-Phase Commit Sink 示例

```java
// 自定义 Two-Phase Commit Sink 示例
public class MyTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<String, String, Void> {

    public MyTwoPhaseCommitSink() {
        super(StringSerializer.INSTANCE, VoidSerializer.INSTANCE);
    }

    @Override
    public void invoke(String value, String transaction, Context context) throws Exception {
        // 在事务中写入数据
        System.out.println("Writing value: " + value);
    }

    @Override
    public String beginTransaction() throws Exception {
        // 创建一个事务并返回事务 ID
        return UUID.randomUUID().toString();
    }

    @Override
    public void preCommit(String transaction) throws Exception {
        // 预提交数据
    }

    @Override
    public void commit(String transaction) throws Exception {
        // 提交事务
        System.out.println("Committing transaction: " + transaction);
    }

    @Override
    public void abort(String transaction) throws Exception {
        // 事务回滚
        System.out.println("Aborting transaction: " + transaction);
    }
}
```

### 使用 Two-Phase Commit Sink

```java
// 使用 Two-Phase Commit Sink 的流式作业
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(5000);  // 启用 Checkpoint，间隔5秒
DataStream<String> dataStream = env.fromElements("data1", "data2", "data3");
dataStream.addSink(new MyTwoPhaseCommitSink());
env.execute("Flink 2.0 Two-Phase Commit Sink Example");
```

这段代码展示了如何使用 Two-Phase Commit Sink，其中 `enableCheckpointing()` 方法用于启用 Checkpoint，这是实现 Exactly-Once 语义的关键。