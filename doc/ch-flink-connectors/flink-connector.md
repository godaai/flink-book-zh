(flink-connector)=
# Flink 中常用的 Connector

本节将对 Flink 常用的 Connector 做一些概括性的介绍，主要包括内置输入 / 输出（Input/Output，I/O）接口、flink-connector 项目所涉及的 Connector、Apache Bahir 所提供的 Connector 等，如 {numref}`fig-flink-connectors` 所示。

```{figure} ./img/Connector.png
---
name: fig-flink-connectors
width: 60%
align: center
---
Flink 中常用的 Connector
```

Flink 支持了绝大多数的常见大数据系统，从系统的类型上，包括了消息队列、数据库、文件系统等；从具体的技术上，包括了 Kafka、Elasticsearch、HBase、Cassandra、JDBC、Kinesis、Redis 等。各个大数据系统使用起来略有不同，接下来将重点介绍一下 Flink 内置 I/O 接口和 Flink Kafka Connector，这两类 Connector 被广泛应用在很多业务场景中，具有很强的代表性。

## 内置 I/O 接口

之所以给这类 Connector 起名为内置 I/O 接口，是因为这些接口直接集成在了 Flink 的核心代码中，无论在任何环境中，我们都可以调用这些接口进行数据输入 / 输出操作。与内置 I/O 接口相对应的是 flink-connector 子项目以及 Apache Bahir 项目中的 Connector，flink-connector 虽然是 Flink 开源项目的一个子项目，但是并没有直接集成到二进制包（我们在第 2 章下载安装的 Flink 安装包）中。因此，使用 Flink 的内置 I/O 接口，一般不需要额外添加依赖，使用其他 Connector 需要添加相应的依赖。

Flink 的内置 I/O 接口如下：

- 基于 Socket 的 Source 和 Sink。
- 基于内存集合的 Source。
- 输出到标准输出的 Sink。
- 基于文件系统的 Source 和 Sink。

在前文中，我们其实已经使用过这里提到的接口，比如从内存集合中创建数据流并将结果输出到标准输出。像 Socket、内存集合和打印这 3 类接口非常适合调试。此外，文件系统被广泛用于大数据的持久化，是大数据架构中经常涉及的一种组件。下面我们将再次梳理一下这些接口，并重点介绍一下基于文件系统的 Source 和 Sink。

### 基于 Socket 的 Source 和 Sink

我们可以从 Socket 数据流中读取和写入数据。

```java
// 读取 Socket 中的数据，数据流数据之间用 \n 来切分
env.socketTextStream(hostname, port, "\n");

// 向 Socket 中写数入据，数据以 SimpleStringSchema 序列化
stream.writeToSocket(outputHost, outputPort, new SimpleStringSchema());
```

由于 Socket 不能保存 Offset，也无法实现数据重发，因此以它作为 Connector 可能会导致故障恢复时的数据丢失，只能提供 At-Most-Once 的投递保障。这种方式非常适合用来调试，开源工具 nc 可以创建 Socket 数据流，结合 Flink 的 Socket 接口可以用来快速验证一些逻辑。

此外，Socket Source 输入数据具有时序性，适合用来调试与时间和窗口有关的程序。

注意，使用 Socket 时，需要提前启动相应的 Socket 端口，以便 Flink 能够建立 Socket 连接，否则将抛出异常。

### 基于内存集合的 Source

最常见调试方式是在内存中创建一些数据列表，并直接写入 Flink 的 Source。

```java
DataStream<Integer> sourceDataStream = env.fromElements(1, 2, 3);
```

它内部调用的是：`fromCollection(Collection<T> data, TypeInformation<T> typeInfo)`。`fromCollection()` 基于 Java 的 Collection 接口。对于一些复杂的数据类型，我们用 Java 的 Collection 来创建数据，并写到 Flink 的 Source 里。

```java
// 获取数据类型
TypeInformation<T> typeInfo = ...
DataStream<T> collectionStream = env.fromCollection(Arrays.asList(data), typeInfo);
```

### 输出到标准输出的 Sink

`print()` 和 `printToErr()` 分别将数据流输出到标准输出流（STDOUT）和标准错误流（STDERR）。这两个方法会调用数据的 `toString()` 方法，将内存对象转换成字符串，因此如果想进行调试、查看结果，一定要实现数据的 `toString()` 方法。Java 的 POJO 类要重写 `toString()` 方法，Scala 的 case class 已经有内置的 `toString()` 方法，无须实现。

```java
public class StockPrice {
    public String symbol;
    public double price;
    public long ts;
    public int volume;
    public String mediaStatus;

    ...

    @Override
    public String toString() {
        return "(" + this.symbol + "," +
                this.price + "," + this.ts +
                "," + this.volume + "," +
                this.mediaStatus + ")";
    }
}
```

`print()` 和 `printToErr()` 方法实际在 TaskManager 上执行，如果并行度大于 1，Flink 会将算子子任务的 ID 一起输出。比如，在 IntelliJ IDEA 中执行程序，可以得到类似下面的结果，每行输出前都有一个数字，该数字表示相应方法实际在哪个算子子任务上执行。

```
1> 490894,1061719,4874384,pv,1512061207
1> 502030,4129946,1567637,pv,1512061207
4> 226011,4228265,3159480,pv,1512057930
4> 228530,3404444,64179,pv,1512057930
6> 694940,4531940,4217906,pv,1512058952
...
```

### 基于文件系统的 Source 和 Sink

#### 基于文件系统的 Source

文件系统一般用来存储数据，为批处理提供输入或输出，是大数据架构中最为重要的组件之一。比如，消息队列可能将一些日志写入文件系统进行持久化，批处理作业从文件系统中读取数据进行分析等。在 Flink 中，基于文件系统的 Source 和 Sink 可以从文件系统中读取和输出数据。

Flink 对各类文件系统都提供了支持，包括本地文件系统以及挂载到本地的网络文件系统（Network File System，NFS）、Hadoop HDFS、Amazon S3、阿里云 OSS 等。Flink 通过路径中的文件系统描述符来确定该文件路径使用什么文件系统，例如 `file:///some/local/file` 或者 `hdfs://host:port/file/path`。

下面的代码从一个文件系统中读取一个文本文件，文件读入后以字符串的形式存在，并生成一个 `DataStream<String>`。

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
String textPath = ...
// readTextFile() 方法默认以 UTF-8 编码格式读取文件
DataStream<String> text = env.readTextFile(textPath);
```

Flink 在内部实际调用的是一个支持更多参数的接口。

```java
/**
  * 从 filePath 文件中读取数据
  * FileInputFormat 定义文件的格式
  * watchType 检测文件路径下的内容是否有更新
  * interval 检测间隔
  */
public <OUT> DataStreamSource<OUT> readFile(
                        FileInputFormat<OUT> inputFormat,
                        String filePath,
                        FileProcessingMode watchType,
                        long interval);
```

上述方法可以读取一个路径下的所有文件。`FileInputFormat` 定义了输入文件的格式，比如一个纯文本文件 `TextInputFormat`，后文还将详细介绍这个接口。参数 `filePath` 是文件路径。如果这个路径指向一个文件，Flink 将读取这个文件，如果这个路径是一个目录，Flink 将读取目录下的文件。基于 `FileProcessingMode`，Flink 提供了如下两种不同的读取文件的模式。

- `FileProcessingMode.PROCESS_ONCE` 模式只读取一遍某个目录下的内容，读取完后随即退出。
- `FileProcessingMode.PROCESS_CONTINUOUSLY` 模式每隔 `interval` 毫秒周期性地检查 `filePath` 路径下的内容是否有更新，如果有更新，重新读取里面的内容。

下面的代码展示了如何调用 `FileInputFormat` 接口。

```java
// 文件路径
String filePath = ...

// 文件为纯文本格式
TextInputFormat textInputFormat = new TextInputFormat(new org.apache.flink.core.fs.Path(filePath));

// 每隔 100 毫秒检测一遍
DataStream<String> inputStream = env.readFile(textInputFormat, filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 100);
```

Flink 在实现文件读取时，增加了一个专门检测文件路径的线程。这个线程启动后定时检测路径下的任何修改，比如是否有文件被修改，或文件夹是否添加了新内容。确切地说，这个线程检测文件的修改时间（Modified Time）是否发生了变化。`FileProcessingMode.PROCESS_CONTINUOUSLY` 模式下 Flink 每隔 `interval` 毫秒周期性地检测文件的修改时间；`FileProcessingMode.PROCESS_ONCE` 只检测一次，不周期性地检测。

:::{note}

重新读取文件内容会影响端到端的 Exactly-Once 一致性。因为检测更新是基于文件的修改时间，如果我们往一个文件中追加数据，文件的修改时间会发生变化，该文件下次检测时会被重新读取，导致一条数据可能会被多次处理。

:::

`FileInputFormat` 是读取文件的基类，继承这个基类可以实现不同类型的文件读取，包括纯文本文件。`TextInputFormat` 是 `FileInputFormat` 的一个实现，`TextInputFormat` 按行读取文件，文件以纯文本的序列化方式打开。Flink 也提供了 `AvroInputFormat`、`OrcInputFormat`、`ParquetInputFormat` 等其他大数据架构所采用的文件格式，这些文件格式比起纯文本文件的性能更好，它们的读 / 写方式也各有不同。

考虑到数据的容量比较大，在实现文件读取的过程中，Flink 会判断 `filePath` 路径下的文件能否切分。假设这个作业的并行度是 `n`，而且文件能够切分，检测线程会将读入的文件切分成 `n` 份，后续启动 `n` 个并行的文件读取实例读取这 `n` 份切分文件。

#### 基于文件系统的 Sink

我们可以使用 `writeAsText(String path)`、`writeAsText(String path, WriteMode writeMode)` 和 `writeUsingOutputFormat(OutputFormat<T> format)` 等方法来将文件输出到文件系统。`WriteMode` 可以为 `NO_OVERWRITE` 和 `OVERWRITE`，即是否覆盖原来路径里的内容。`OutputFormat` 与 `FileInputFormat` 类似，表示目标文件的文件格式。在最新的 Flink 版本中，这几个输出到文件系统的方法被标记为 `@Deprecated`，表示未来将被弃用，主要考虑到这些方法没有参与 Flink 的 Checkpoint 过程中，无法提供 Exactly-Once 保障。这些方法适合用于本地调试。

在生产环境中，为了保证数据的一致性，官方建议使用 `StreamingFileSink` 接口。下面这个例子展示了如何将一个文本数据流输出到一个目标路径上。这里用到的是一个非常简单的配置，包括一个文件路径和一个 `Encoder`。`Encoder` 可以将数据编码以便对数据进行序列化。

```java
DataStream<Address> stream = env.addSource(...);

// 使用 StreamingFileSink 将 DataStream 输出为一个文本文件
StreamingFileSink<String> fileSink = StreamingFileSink
  .forRowFormat(new Path("/file/base/path"), new SimpleStringEncoder<String>("UTF-8"))
  .build();
stream.addSink(fileSink);
```

`StreamingFileSink` 主要支持两类文件，一种是行式存储，一种是列式存储。我们平时见到的很多数据是行式存储的，即在文件的末尾追加新的行。列式存储在某些场景下的性能很高，它将一批数据收集起来，批量写入。行式存储和列式存储的接口如下。

- 行式存储：`StreamingFileSink.forRowFormat(basePath, rowEncoder)`。
- 列式存储：`StreamingFileSink.forBulkFormat(basePath, bulkWriterFactory)`。

回到刚才的例子上，它使用了行式存储，`SimpleStringEncoder` 是 Flink 提供的预定义的 `Encoder`，它通过数据流的 `toString()` 方法将内存数据转换为字符串，将字符串按照 UTF-8 编码写入输出中。`SimpleStringEncoder<String>` 可以用来编码转换字符串数据流，`SimpleStringEncoder<Long>` 可以用来编码转换长整数数据流。

如果数据流比较复杂，我们需要自己实现一个 `Encoder`。代码清单 7-7 中的数据流是一个 `DataStream<Tuple2<String, Integer>>`，我们需要实现 `encode()` 方法，将每个数据编码。

```java
// 将一个二元组数据流编码并序列化
static class Tuple2Encoder implements Encoder<Tuple2<String, Integer>> {
    @Override
    public void encode(Tuple2<String, Integer> element, OutputStream stream) throws IOException {
        stream.write((element.f0 + '@' + element.f1).getBytes(StandardCharsets.UTF_8));
        stream.write('\n');
    }
}
```

对于列式存储，也需要一个类似的 `Encoder`，Flink 称之为 `BulkWriter`，本质上将数据序列化为列式存储所需的格式。比如我们想使用 Parquet 格式，代码如下。

```java
DataStream<Datum> stream = ...;

StreamingFileSink<Datum> fileSink = StreamingFileSink
  .forBulkFormat(new Path("/file/base/path"), ParquetAvroWriters.forReflectRecord(Datum.class))
  .build();

stream.addSink(fileSink);
```

考虑到大数据场景下，输出数据量会很大，而且流处理作业需要长时间执行，`StreamingFileSink` 的具体实现过程中使用了桶的概念。桶可以理解为输出路径的一个子文件夹。如果不做其他设置，Flink 按照时间来将输出数据分桶，会在输出路径下生成类似下面的文件夹结构。

```
/file/base/path
└── 2020-02-25--15
    ├── part-0-0.inprogress.92c7be6f-8cfc-4ca3-905b-91b0e20ba9a9
    ├── part-1-0.inprogress.18f9fa71-1525-4776-a7bc-fe02ee1f2dda
```

目录和文件名实际上是按照下面的结构来命名的。

```
[base-path]/[bucket-path]/part-[task-id]-[id]
```

最顶层的文件夹是我们设置的输出目录，第二层是桶，Flink 将当前的时间作为 `bucket-path` 桶名。实际输出时，Flink 会启动多个并行的实例，每个实例有自己的 `task-id`，`task-id` 被添加在了 `part` 之后。

我们也可以自定义数据分配的方式，将某一条数据分配到相应的桶中。

```java
StreamingFileSink<String> fileSink = StreamingFileSink
  .forRowFormat(new Path("/file/path"), new SimpleStringEncoder<String>("UTF-8"))
  .withBucketAssigner(new DateTimeBucketAssigner<>())
  .build();
```

上述的文件夹结构中，有“inprogress”字样，这与 `StreamingFileSink` 能够提供的 Exactly-Once 保障有关。一份数据从生成到最终可用需要经过 3 个阶段：进行中（In-progress）、等待（Pending）和结束（Finished）。当数据刚刚生成时，文件处于 In-progress 阶段；当数据已经准备好（比如单个 part 文件足够大），文件被置为 Pending 阶段；下次 Checkpoint 执行完，整个作业的状态数据是一致的，文件最终被置为 Finished 阶段，Finished 阶段的文件名没有“inprogress”的字样。从这个角度来看，`StreamingFileSink` 和 Checkpoint 机制结合，能够提供 Exactly-Once 保障。

## Flink Kafka Connector

在第 1 章中我们曾提到，Kafka 是一个消息队列，它可以在 Flink 的上游向 Flink 发送数据，也可以在 Flink 的下游接收 Flink 的输出。Kafka 是一个很多公司都采用的消息队列，因此非常具有代表性。

Kafka 的 API 经过不断迭代，已经趋于稳定，我们接下来主要介绍基于稳定版本的 Kafka Connector。如果仍然使用较旧版本的 Kafka（0.11 或更旧的版本），可以通过官方文档来了解具体的使用方法。由于 Kafka Connector 并没有内置在 Flink 核心程序中，使用之前，我们需要在 Maven 中添加依赖。

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-kafka_${scala.binary.version}</artifactId>
  <version>${flink.version}</version>
</dependency>
```

### Flink Kafka Source

Kafka 作为一个 Flink 作业的上游，可以为该作业提供数据，我们需要一个可以连接 Kafka 的 Source 读取 Kafka 中的内容，这时 Kafka 是一个 Producer，Flink 作为 Kafka 的 Consumer 来消费 Kafka 中的数据。代码清单 7-8 展示了如何初始化一个 Kafka Source Connector。

```java
// Kafka 参数
Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("group.id", "flink-group");
String inputTopic = "Shakespeare";

// Source
FlinkKafkaConsumer<String> consumer =
  new FlinkKafkaConsumer<String>(inputTopic, new SimpleStringSchema(), properties);
DataStream<String> stream = env.addSource(consumer);
```

代码清单 7-8  初始化 Kafka Source Consumer

代码清单 7-8 创建了一个 FlinkKafkaConsumer，它需要 3 个参数：Topic、反序列化方式和 Kafka 相关参数。Topic 是我们想读取的具体内容，是一个字符串，并且可以支持正则表达式。Kafka 中传输的是二进制数据，需要提供一个反序列化方式，将数据转化为具体的 Java 或 Scala 对象。Flink 已经提供了一些序列化实现，比如：SimpleStringSchema 按照字符串进行序列化和反序列化，JsonNodeDeserializationSchema 使用 Jackson 对 JSON 数据进行序列化和反序列化。如果数据类型比较复杂，我们需要实现 DeserializationSchema 或者 KafkaDeserializationSchema 接口。最后一个参数 Properties 是 Kafka 相关的设置，用来配置 Kafka 的 Consumer，我们需要配置 bootstrap.servers 和 group.id，其他的参数可以参考 Kafka 的文档进行配置。

Flink Kafka Consumer 可以配置从哪个位置读取消息队列中的数据。默认情况下，从 Kafka Consumer Group 记录的 Offset 开始消费，Consumer Group 是根据 group.id 所配置的。其他配置可以参考下面的代码。

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(...);
consumer.setStartFromGroupOffsets(); // 默认从 Kafka 记录中的 Offset 开始
consumer.setStartFromEarliest();     // 从最早的数据开始
consumer.setStartFromLatest();       // 从最近的数据开始
consumer.setStartFromTimestamp(...); // 从某个时间戳开始

DataStream<String> stream = env.addSource(consumer);
```

:::{note}

上述代码中配置消费的起始位置只影响作业第一次启动时所应读取的位置，不会影响故障恢复时重新消费的位置。

:::

如果作业启用了 Flink 的 Checkpoint 机制，Checkpoint 时会记录 Kafka Consumer 消费到哪个位置，或者说记录了 Consumer Group 在该 Topic 下每个分区的 Offset。如果遇到故障恢复，Flink 会从最近一次的 Checkpoint 中恢复 Offset，并从该 Offset 重新消费 Kafka 中的数据。可见，Flink Kafka Consumer 是支持数据重发的。

### Flink Kafka Sink

Kafka 作为 Flink 作业的下游，可以接收 Flink 作业的输出，这时我们可以通过 Kafka Sink 将处理好的数据输出到 Kafka 中。在这种场景下，Flink 是生成数据的 Producer，向 Kafka 输出。
比如我们将 WordCount 程序结果输出到一个 Kafka 数据流中。

```java
DataStream<Tuple2<String, Integer>> wordCount = ...

FlinkKafkaProducer<Tuple2<String, Integer>> producer = new 
FlinkKafkaProducer<Tuple2<String, Integer>> (
      outputTopic,
      new KafkaWordCountSerializationSchema(outputTopic),
    properties,
    FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
wordCount.addSink(producer);
```

上面的代码创建了一个 FlinkKafkaProducer，它需要 4 个参数：Topic、序列化方式、连接 Kafka 的相关参数以及选择什么样的投递保障。这些参数中，Topic 和连接的相关 Kafka 参数与前文所述的内容基本一样。

序列化方式与前面提到的反序列化方式相对应，它主要将 Java 或 Scala 对象转化为可在 Kafka 中传输的二进制数据。这个例子中，我们要传输的是一个 Tuple2<String, Integer>，需要提供对这个数据类型进行序列化的代码，例如代码清单 7-9 的序列化代码。

```java
public static class KafkaWordCountSerializationSchema implements 
KafkaSerializationSchema<Tuple2<String, Integer>> {

      private String topic;

    public KafkaWordCountSerializationSchema(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Integer> element, Long timestamp) {
        return new ProducerRecord<byte[], byte[]>(topic, (element.f0 + ": " + 
element.f1).getBytes(StandardCharsets.UTF_8));
    }
}
```

代码清单 7-9  将数据写到 Kafka Sink 时，需要进行序列化

最后一个参数决定了 Flink Kafka Sink 以什么样的语义来保障数据写入 Kafka，它接受 FlinkKafkaProducer.Semantic 的枚举类型，有 3 种类型：NONE、AT_LEAST_ONCE 和 EXACTLY_ONCE。
- None：不提供任何保障，数据可能会丢失也可能会重复。
- AT_LEAST_ONCE：保证不丢失数据，但是有可能会重复。
- EXACTLY_ONCE：基于 Kafka 提供的事务写功能，一条数据最终只写入 Kafka 一次。

其中，EXACTLY_ONCE 基于 Kafka 提供的事务写功能，使用了我们提到的 Two-Phase-Commit 协议，它保证了数据端到端的 Exactly-Once 保障。当然，这个类型的代价是输出延迟会增大。实际执行过程中，这种方式比较依赖 Kafka 和 Flink 之间的协作，如果 Flink 作业的故障恢复时间过长，Kafka 不会长时间保存事务中的数据，有可能发生超时，最终也可能会导致数据丢失。AT_LEAST_ONCE 是默认的，它不会丢失数据，但数据有可能是重复的。
