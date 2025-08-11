(table-overview)=
# Table API & SQL 综述

:::{note}

本教程已出版为《Flink 原理与实践》，感兴趣的读者请在各大电商平台购买！

<a href="https://item.jd.com/13154364.html"> ![](https://img.shields.io/badge/JD-%E8%B4%AD%E4%B9%B0%E9%93%BE%E6%8E%A5-red) </a>


:::

Flink 的 Table API 和 SQL 提供了一种更高级、更声明式的方式来处理数据，无论是流数据还是批数据。这两种 API 非常相似，都像是操作关系型数据库里的表一样，它们的核心都围绕着一个叫做 `Table` 的抽象概念。在底层，Flink 会利用一个强大的查询优化器（Planner）把你的 Table API 调用或者 SQL 语句转换成高效的可执行 Flink 作业。好消息是，从 Flink 2.0 开始，内部的 Planner 已经统一，我们不再需要关心具体用的是哪个 Planner 了。不过，Table API 和 SQL 的发展非常快，建议随时关注 Flink 官方文档以获取最新的用法和特性。

这一节，我们就来聊聊一个典型的 Table API & SQL 程序大概长什么样，以及如何让 Flink 连接到外部数据系统。

## Table API & SQL 程序骨架结构

我们来看一个简单的例子，感受一下 Table API & SQL 程序的基本流程。假设我们要处理来自 Kafka 的用户行为数据，并把结果写回 Kafka。整个过程大致如下：

```java
// 1. 获取执行环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// 2. 创建 TableEnvironment
TableEnvironment tEnv = TableEnvironment.create(env);

// 3. 定义数据源 (Source Table)，例如使用 SQL DDL 从 Kafka 读取
// 具体 DDL 语句见下文 “获取 Table” 部分
tEnv.executeSql("CREATE TABLE user_behavior (...) WITH (...)");

// 4. 定义数据汇 (Sink Table)，例如使用 SQL DDL 写入 Kafka
tEnv.executeSql("CREATE TABLE output_kafka (...) WITH (...)");

// 5. 执行查询操作，例如用 SQL 统计每分钟的 UV
// 具体的 SQL 查询可以参考 chapter8_sql 下的 UserBehaviorUVPerMin.java
Table resultTable = tEnv.sqlQuery(
    "SELECT TUMBLE_START(ts, INTERVAL '1' MINUTE) as window_start, COUNT(DISTINCT user_id) as uv " +
    "FROM user_behavior " +
    "GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE)"
);

// 6. 将结果写入 Sink Table
resultTable.executeInsert("output_kafka");

// 7. 执行作业 (注意：对于 DDL 和 executeInsert，通常不需要再调用 tEnv.execute())
// 如果只有查询和打印等操作，则需要调用 tEnv.execute("My Table Job");
```

总的来说，一个 Table API & SQL 作业主要包含这么几个环节：创建环境、定义数据来源（创建源 `Table`）、定义数据去向（创建目标 `Table`）、编写查询逻辑（使用 Table API 或 SQL），最后把查询结果发送到目标 `Table`。如果你的作业只包含 DDL 和 `executeInsert` 操作，Flink 会自动处理执行；如果包含像 `select` 后打印结果这样的操作，则需要显式调用 `execute()` 来启动作业。


## 创建 TableEnvironment

`TableEnvironment` 是 Table API & SQL 世界的入口，它管理着你的表、函数、配置等等核心信息。在 Flink 2.0 里，创建 `TableEnvironment` 非常直接。正如你在 `chapter8_sql` 目录下的很多 Java 示例代码（比如 `UserBehaviorFromDataStream.java` 或 `UserBehaviorFromFile.java`）中看到的那样，最常见的方式就是直接从 `StreamExecutionEnvironment` 创建：

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
TableEnvironment tEnv = TableEnvironment.create(env);
```

这种方式创建的 `TableEnvironment` 会继承 `StreamExecutionEnvironment` 的配置。当然，如果你需要更精细地控制环境配置，比如指定是流模式还是批模式（虽然 Flink 正在努力统一这两者），可以使用 `EnvironmentSettings` 来创建：

```java
EnvironmentSettings settings = EnvironmentSettings
    .newInstance()
    // .inStreamingMode() // 或者 .inBatchMode()
    .build();
TableEnvironment tEnv = TableEnvironment.create(settings);
```

不过，对于大多数流处理场景，直接从 `StreamExecutionEnvironment` 创建通常就足够了。


## 获取 Table

有了 `TableEnvironment` 之后，下一步就是告诉 Flink 数据在哪里，也就是获取 `Table` 对象。Flink 把各种数据源都抽象成了 `Table`，方便我们用统一的方式查询。获取 `Table` 主要有这么几种途径：

**1. 连接外部系统:** 这是最常见的方式。Flink 通过连接器（Connector）来读写外部系统的数据。你可以使用 `connect()` API 来指定连接器的类型、所需的配置参数（比如 Kafka 的地址、topic，或者文件的路径）以及数据的 Schema。这种方式创建的表是临时表（Temporary Table），它的生命周期和你的 Flink 作业绑定在一起。`UserBehaviorKafkaConnect.java` 就是一个使用 `connect()` API 从 Kafka 读取 JSON 数据的例子：

```java
// UserBehaviorKafkaConnect.java 示例片段
tEnv.connect(
        new Kafka()
            .version("universal") // 指定 Kafka 版本
            .topic("user_behavior") // 指定 Topic
            .property("zookeeper.connect", "localhost:2181") // Kafka 连接属性
            .property("bootstrap.servers", "localhost:9092")
            .startFromLatest() // 从最新的 offset 开始消费
    )
    .withFormat(
        new Json()
            .failOnMissingField(false) // JSON 解析设置
            .jsonSchema("{\n" + // 定义 JSON 数据的 Schema
                         "  'type': 'object',\n" +
                         "  'properties': {\n" +
                         "    'user_id': {'type': 'integer'},\n" +
                         "    'item_id': {'type': 'integer'},\n" +
                         "    'category_id': {'type': 'integer'},\n" +
                         "    'behavior': {'type': 'string'},\n" +
                         "    'ts': {'type': 'string', 'format': 'date-time'}\n" +
                         "  }\n" +
                         "}")
    )
    .withSchema(
        new Schema()
            .field("user_id", DataTypes.BIGINT()) // 定义 Table 的 Schema
            .field("item_id", DataTypes.BIGINT())
            .field("category_id", DataTypes.BIGINT())
            .field("behavior", DataTypes.STRING())
            .field("ts", DataTypes.TIMESTAMP(3)) // 时间戳字段
            .proctime("proc_time") // 定义处理时间属性
            .rowtime(new Rowtime().timestampsFromField("ts").watermarksPeriodicBounded(60000)) // 定义事件时间和 Watermark
    )
    .createTemporaryTable("user_behavior"); // 创建临时表
```
同样，`UserBehaviorFromFile.java` 也展示了如何用 `connect` API 连接文件系统来读取 CSV 数据。

**2. 使用 SQL DDL:** 另一种非常流行且灵活的方式是使用 SQL 的 `CREATE TABLE` 语句（DDL）。`UserBehaviorFromKafkaSQLDDL.java` 就是一个很好的例子。你可以在 SQL 语句里直接定义表的 Schema、选择连接器类型（`'connector' = 'kafka'`）、配置所有连接参数，甚至包括 Watermark 的定义。这种方式更加声明式，写起来就像在操作传统数据库一样，而且通过 Catalog 管理，这些表定义可以更容易地被复用，甚至创建成跨作业、跨会话使用的永久表（Permanent Table）。

```sql
-- UserBehaviorFromKafkaSQLDDL.java 示例 SQL
CREATE TABLE user_behavior (
    user_id BIGINT,
    item_id BIGINT,
    category_id BIGINT,
    behavior STRING,
    ts TIMESTAMP(3),
    -- 基于 ts 字段定义 Watermark，允许 5 秒的延迟
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND 
) WITH (
    'connector' = 'kafka', -- 指定连接器为 Kafka
    'topic' = 'user_behavior', -- Kafka Topic
    'properties.bootstrap.servers' = 'localhost:9092', -- Kafka 连接地址
    'properties.group.id' = 'testGroup', -- Consumer Group ID
    'format' = 'json', -- 数据格式为 JSON
    'scan.startup.mode' = 'earliest-offset' -- 从最早的 offset 开始读取
);
```

**3. 从 DataStream/DataSet 转换:** 如果你的数据已经存在于 Flink 的 `DataStream` (流处理) 或 `DataSet` (批处理，未来会被 DataStream 统一) 对象中了，那么转换成 `Table` 非常简单。`UserBehaviorFromDataStream.java` 就展示了如何使用 `fromDataStream()` 方法。你只需要提供 `DataStream` 对象，并可以指定 Schema，包括定义时间属性（处理时间或事件时间）和 Watermark。

```java
// UserBehaviorFromDataStream.java 示例片段
DataStream<UserBehavior> userBehaviorStream = env.addSource(...); // 假设已有 DataStream

// 将 DataStream 转换成 Table
Table userBehaviorTable = tEnv.fromDataStream(
    userBehaviorStream,
    // 使用 Schema.newBuilder() 更清晰地定义 Schema 和时间属性
    Schema.newBuilder()
            .column("user_id", DataTypes.BIGINT())
            .column("item_id", DataTypes.BIGINT())
            .column("category_id", DataTypes.BIGINT())
            .column("behavior", DataTypes.STRING())
            .column("ts", DataTypes.TIMESTAMP(3))
            // 定义事件时间戳字段和 Watermark 生成策略
            .watermark("ts", "ts - INTERVAL '5' SECOND") 
            .build()
);

// 可以选择将转换后的 Table 注册为一个临时视图 (View)
tEnv.createTemporaryView("user_behavior_view", userBehaviorTable);
```
通过这种方式得到的 `Table` 也是临时的，可以立即在后续的 Table API 或 SQL 查询中使用。

**4. 注册 Catalog Table:** 对于已经通过 Catalog 服务（如 Hive Metastore 或 Flink 内建的 GenericInMemoryCatalog）预先定义好的永久表（Permanent Table），你可以直接在 `TableEnvironment` 中使用它们的名字来获取对应的 `Table` 对象，就像使用数据库里的表一样。

总之，无论数据来自哪里，目标都是得到一个 `Table` 对象，这样我们就可以开始进行数据处理和分析了。

## 在 Table 上执行语句

一旦你手里有了一个或多个 `Table` 对象，就可以施展拳脚，对数据进行各种转换和计算了。Flink 提供了两种方式来操作 `Table`：

**1. Table API:** 这是一种函数式的、支持链式调用的 API。你可以像操作 Java/Scala 集合一样，调用 `select()`, `filter()`, `where()`, `groupBy()`, `join()`, `window()` 等方法。这种方式写起来比较符合面向对象编程的习惯，而且有编译时的类型检查。

```java
// Table API 示例 (类似 UserBehaviorUVPerMin.java 中的逻辑)
Table userBehavior = tEnv.from("user_behavior"); // 假设已获取 user_behavior 表

Table resultTable = userBehavior
    .window(Tumble.over(lit(1).minutes()).on($("ts")).as("w")) // 定义1分钟滚动窗口
    .groupBy($("w")) // 按窗口分组
    .select(
        $("w").start().as("window_start"), // 选择窗口开始时间
        $("user_id").countDistinct().as("uv") // 计算 UV (Distinct User Count)
    );
```

**2. SQL:** Flink 对标准 SQL 的支持非常强大和完善。你可以使用 `tableEnv.sqlQuery()` 来执行一个 `SELECT` 语句，或者用 `tableEnv.executeSql()` 来执行 DDL（如 `CREATE TABLE`, `CREATE VIEW`, `CREATE FUNCTION`）或 DML（如 `INSERT INTO`）。SQL 的表达能力强，对于熟悉数据库操作的人来说非常自然。Flink 2.0 支持包括窗口聚合（TUMBLE, HOP, SESSION）、复杂的 Join、子查询、分析函数（窗口函数，如 ROW_NUMBER, RANK）、集合操作（UNION ALL）、以及 CTE（Common Table Expressions，即 `WITH` 子句）等众多高级 SQL 特性。

```sql
// SQL 示例 (UserBehaviorUVPerMin.java 中的 SQL)
Table resultTable = tEnv.sqlQuery(
    "SELECT " +
    "  TUMBLE_START(ts, INTERVAL '1' MINUTE) AS window_start, " +
    "  COUNT(DISTINCT user_id) AS uv " +
    "FROM user_behavior " +
    // 使用 TUMBLE 函数定义1分钟滚动窗口
    "GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE)"
);
```

一个特别棒的地方是，Table API 和 SQL 可以无缝混合使用。你可以用 Table API 操作得到一个中间结果 `Table`，然后在这个 `Table` 上继续执行 SQL 查询；反之亦然。这两种方式最终都作用于 `Table` 对象，并且查询的结果通常也是一个新的 `Table` 对象。

## 将 Table 结果输出

计算出了结果 `Table` 之后，我们通常需要把它写到某个地方，比如另一个 Kafka Topic、一个文件系统、或者一个数据库。这就要用到 `TableSink` 了。和 `TableSource` 类似，你需要定义一个目标表（Sink Table），告诉 Flink 数据要写到哪里、以什么格式写。

同样，你可以用 `connect()` API 或者 SQL DDL `CREATE TABLE` 来定义 Sink Table。定义好 Sink Table 后，就可以使用 `executeInsert()` 方法（这是 Flink 1.11 引入的推荐方式）将你的结果 `Table` 插入到目标表中。

```java
// 假设 resultTable 是我们前面查询得到的结果 Table
// 假设我们已经用 DDL 创建了一个名为 output_kafka 的 Sink Table

// 将 resultTable 的数据插入到 output_kafka 表
TableResult tableResult = resultTable.executeInsert("output_kafka");

// 可以选择等待作业完成 (如果需要)
// tableResult.await(); 
```

或者，如果你用 `connect()` API 定义了 Sink，也可以用 `insertInto()` (旧版 API，仍可用但推荐 `executeInsert`)：

```java
// 假设用 connect() 定义了一个名为 sinkTable 的 Sink
// tEnv.connect(...).createTemporaryTable("sinkTable");

// resultTable.insertInto("sinkTable"); // 旧版 API
// tEnv.execute("Insert Job"); // 旧版 API 需要手动执行
```

`executeInsert()` 的好处是它会立即提交一个 Flink 作业来执行插入操作，并且返回一个 `TableResult` 对象，你可以用它来获取作业信息或等待作业完成。


## 执行作业

最后一步，别忘了让 Flink 真正动起来。正如前面提到的，如果你使用了 `executeInsert()` 或者 `executeSql()` 执行 DML 或 DDL，Flink 通常会自动提交并执行相应的作业。但是，如果你的程序只包含 `sqlQuery()` 或 Table API 的转换操作，并且你想看到结果（比如打印到控制台，这通常只在测试或调试时使用），或者你的作业没有包含自动触发执行的操作，那么你需要显式调用 `tableEnv.execute("Your Job Name")` 来启动整个 Flink 作业。

```java
// 如果只是查询并打印结果 (仅建议调试时用)
Table result = tEnv.sqlQuery("SELECT ... FROM ...");
result.execute().print(); // execute() 触发执行并获取结果

// 或者，如果定义了完整的 Source -> Sink 流程但未使用 executeInsert
// tableEnv.execute("My Complete Flink Table Job"); 
```

至此，一个完整的 Table API & SQL 作业就构建并运行起来了。