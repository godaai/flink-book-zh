(sql-ddl)=
# SQL DDL

:::{note}

本教程已出版为《Flink 原理与实践》，感兴趣的读者请在各大电商平台购买！

<a href="https://item.jd.com/13154364.html"> ![](https://img.shields.io/badge/JD-%E8%B4%AD%E4%B9%B0%E9%93%BE%E6%8E%A5-red) </a>


:::

在前面的章节中，我们已经了解了如何使用 Flink SQL 查询数据（`SELECT` 语句），以及动态表和持续查询的概念。但是，在能够查询之前，我们需要先告诉 Flink **去哪里找数据**、**数据的结构是怎样的**，以及**如何读写这些数据**。这就是数据定义语言（Data Definition Language, DDL）的作用。

Flink SQL 支持标准的 SQL DDL 语句，允许我们创建、修改、删除和管理各种元数据对象，如 Catalog、数据库（Database）、表（Table）、视图（View）和函数（Function）。本节将重点介绍如何使用 DDL 来管理这些元数据，特别是与表定义相关的核心概念。

## Catalog：元数据的组织者

在 Flink 中，**Catalog** 是组织和管理元数据的顶层结构。你可以把它想象成一个包含多个数据库（Database）的容器，而每个数据库又可以包含多个表（Table）、视图（View）和函数（Function）。Catalog 提供了一套 API，用于注册、查找和访问这些元数据信息。

Catalog 的层级结构如下：

```
Catalog
└── Database
    ├── Table
    ├── View
    └── Function
```

Flink 内置了几种 Catalog 实现，最常用的是：

*   **`GenericInMemoryCatalog`**: 这是 Flink **默认**使用的 Catalog。顾名思义，它将所有的元数据信息（数据库、表、函数等）存储在**内存**中。这意味着一旦 Flink 的会话（Session）结束，所有通过这个 Catalog 注册的元数据都会**丢失**。它非常适合快速原型开发和测试，或者那些不需要跨会话共享元数据的场景。默认情况下，`GenericInMemoryCatalog` 包含一个名为 `default_database` 的默认数据库。

*   **`HiveCatalog`**: 为了与 Hadoop 生态系统（特别是 Hive Metastore）无缝集成，Flink 提供了 `HiveCatalog`。它使用 Hive Metastore 来**持久化**存储元数据。这意味着即使 Flink 会话结束，注册在 `HiveCatalog` 中的数据库、表和函数信息依然存在，可以在后续的会话中直接使用。
    *   对于已经部署了 Hive 的环境，`HiveCatalog` 允许 Flink 直接访问和查询 Hive 中已有的表。
    *   对于只部署了 Flink 的环境，`HiveCatalog` 是目前**官方推荐的持久化元数据的标准方式**。通过它，数据管理团队可以预先定义好数据源表（如 Kafka 主题、HDFS 目录），数据分析团队则可以直接使用这些表，无需每次都重复定义连接信息和表结构。

*   **用户自定义 Catalog**: Flink 也允许用户通过实现 `Catalog` 接口来创建自定义的 Catalog，以对接其他元数据管理系统。

### 注册和使用 Catalog

你可以在 `TableEnvironment` 中注册和切换 Catalog。

```java
import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.hive.HiveCatalog;

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

// 1. 使用默认的 GenericInMemoryCatalog
// Flink 默认创建了一个名为 default_catalog 的 GenericInMemoryCatalog
tEnv.useCatalog("default_catalog");
tEnv.useDatabase("default_database");

// 2. 注册并使用 HiveCatalog (需要添加 flink-connector-hive 依赖)
String catalogName    = "myhive";        // Catalog 名称
String defaultDatabase = "my_db";        // Hive中的默认数据库
String hiveConfDir  = "/path/to/hive/conf"; // Hive 配置文件目录 (包含 hive-site.xml)
// String hiveVersion = "3.1.2"; // 如果需要，指定 Hive 版本

HiveCatalog hiveCatalog = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir);
tEnv.registerCatalog(catalogName, hiveCatalog);

// 切换到 Hive Catalog
tEnv.useCatalog(catalogName);
// 切换到 Hive Catalog 中的某个数据库 (如果 defaultDatabase 不存在，可能需要先创建)
// tEnv.executeSql("CREATE DATABASE IF NOT EXISTS some_other_db");
// tEnv.useDatabase("some_other_db");
```

## 管理 Catalog 和 Database (`SHOW`, `USE`)

一旦注册了 Catalog，就可以使用标准的 SQL 语句来查看和切换当前的 Catalog 和 Database。

```sql
-- 显示当前可用的所有 Catalog
SHOW CATALOGS;
--> default_catalog
--> myhive

-- 显示当前的 Catalog
SHOW CURRENT CATALOG;
--> default_catalog

-- 切换到 myhive Catalog
USE CATALOG myhive;

-- 显示当前 Catalog 下的所有数据库
SHOW DATABASES;
--> default
--> my_db

-- 显示当前的 Database
SHOW CURRENT DATABASE;
--> my_db

-- 切换到 my_db 数据库
USE my_db;

-- 显示当前 Catalog 和 Database 下的所有表
SHOW TABLES;
--> table1
--> table2
```

这些 `SHOW` 和 `USE` 语句可以通过 `TableEnvironment` 的 `executeSql()` 方法执行，或者在 SQL 客户端（SQL Client）中使用。

```java
// 切换 Catalog 和 Database
tEnv.executeSql("USE CATALOG myhive");
tEnv.executeSql("USE my_db");

// 显示表
TableResult result = tEnv.executeSql("SHOW TABLES");
result.print(); // 打印结果
```

## 管理数据库、表和函数 (`CREATE`, `DROP`, `ALTER`)

Flink SQL 支持标准的 `CREATE`, `DROP`, `ALTER` 语句来管理数据库、表和函数。

### 管理数据库

*   `CREATE DATABASE [IF NOT EXISTS] [catalog_name.]db_name [COMMENT database_comment] [WITH (key1=val1, ...)]`
    *   创建一个新的数据库。`IF NOT EXISTS` 避免了数据库已存在时的错误。
    *   `WITH` 子句可以设置数据库级别的属性。
*   `DROP DATABASE [IF EXISTS] [catalog_name.]db_name [RESTRICT | CASCADE]`
    *   删除一个数据库。`IF EXISTS` 避免了数据库不存在时的错误。
    *   `RESTRICT`（默认）：如果数据库不为空（包含表、视图或函数），则删除失败。
    *   `CASCADE`：强制删除数据库及其包含的所有对象。
*   `ALTER DATABASE [catalog_name.]db_name SET (key1=val1, ...)`
    *   修改数据库的属性。

```java
tEnv.executeSql("CREATE DATABASE IF NOT EXISTS my_new_db WITH ('prop1' = 'value1')");
tEnv.executeSql("ALTER DATABASE my_new_db SET ('prop1' = 'new_value', 'prop2' = 'value2')");
tEnv.executeSql("DROP DATABASE IF EXISTS my_old_db CASCADE");
```

### 管理函数

*   `CREATE [TEMPORARY|TEMPORARY SYSTEM] FUNCTION [IF NOT EXISTS] [catalog_name.][db_name.]function_name AS class_name [LANGUAGE JAVA|SCALA]`
    *   注册一个用户自定义函数（UDF）。`TEMPORARY` 表示函数只在当前会话有效（注册在内存 Catalog 中），`TEMPORARY SYSTEM` 表示在所有内存 Catalog 的数据库中都可用。省略 `TEMPORARY` 则表示在当前 Catalog 和 Database 中持久化注册（如果 Catalog 支持持久化）。
*   `DROP [TEMPORARY|TEMPORARY SYSTEM] FUNCTION [IF EXISTS] [catalog_name.][db_name.]function_name`
    *   删除一个 UDF。
*   `ALTER FUNCTION [catalog_name.][db_name.]function_name AS class_name [LANGUAGE JAVA|SCALA]`
    *   修改一个已注册的 UDF（通常是更新其实现类）。

我们将在后续章节详细讨论用户自定义函数。

### 管理表 (`CREATE TABLE` 详解)

`CREATE TABLE` 是最常用的 DDL 语句之一，用于定义表的结构、连接器信息、格式化方式以及其他属性。

```sql
CREATE TABLE [IF NOT EXISTS] [catalog_name.][db_name.]table_name
  (
    -- 列定义
    { <column_definition> | <computed_column_definition> }[ , ...n ]
    -- 可选：主键约束
    [ , PRIMARY KEY (column_name, ...) NOT ENFORCED ]
    -- 可选：Watermark 定义
    [ , <watermark_definition> ]
  )
  [ COMMENT table_comment ]
  [ PARTITIONED BY (partition_column_name1, partition_column_name2, ...) ] -- 可选：分区
  WITH (
    'connector' = 'connector_identifier', -- 指定连接器类型
    -- 连接器和格式化所需的其他选项
    key1 = val1,
    key2 = val2,
    ...
  )
```

让我们分解这个语法：

1.  **`[IF NOT EXISTS] [catalog_name.][db_name.]table_name`**: 指定表的完整名称。`IF NOT EXISTS` 可以避免表已存在时出错。如果省略 Catalog 和 Database 名称，则使用当前 `USE` 语句指定的 Catalog 和 Database。

2.  **列定义 (`<column_definition>`)**: 定义表的普通列。
    ```sql
    column_name <column_type> [COMMENT column_comment]
    ```
    *   `column_name`: 列名。
    *   `<column_type>`: 列的数据类型，例如 `INT`, `BIGINT`, `STRING`, `DOUBLE`, `TIMESTAMP(3)`, `ROW<f1 INT, f2 STRING>`, `ARRAY<STRING>`, `MAP<STRING, INT>` 等。Flink 支持标准 SQL 类型以及嵌套、复杂类型。
    *   `COMMENT`: 可选的列注释。

3.  **计算列 (`<computed_column_definition>`)**: 定义基于其他列计算得出的虚拟列。计算列不实际存储数据，而是在查询时动态计算。
    ```sql
    column_name AS <expression> [COMMENT column_comment]
    ```
    *   `expression`: 一个 SQL 表达式，可以引用同一张表中的其他列。计算列常用于简化查询或预处理数据。
    *   **定义处理时间**: 一个常见的用途是定义**处理时间（Processing Time）**属性：
        ```sql
        proc_time AS PROCTIME()
        ```
        这里 `PROCTIME()` 是 Flink 内置函数，`proc_time` 列就代表了记录被处理时机器的系统时间。

4.  **主键约束 (`PRIMARY KEY ... NOT ENFORCED`)**: 可选地定义表的主键。Flink 使用主键信息进行优化（例如，用于 Upsert 模式的 Sink），但**并不强制执行**主键的唯一性约束 (`NOT ENFORCED` 是必需的)。

5.  **Watermark 定义 (`<watermark_definition>`)**: 定义**事件时间（Event Time）**属性和 Watermark 生成策略，这对于处理乱序事件至关重要。
    ```sql
    WATERMARK FOR <event_time_column> AS <watermark_strategy_expression>
    ```
    *   `<event_time_column>`: 指定哪个 `TIMESTAMP` 或 `TIMESTAMP_LTZ` 类型的列作为事件时间的来源。
    *   `<watermark_strategy_expression>`: 定义 Watermark 的生成逻辑。常见的策略有：
        *   **有界乱序 (Bounded Out-of-Orderness)**: `ts - INTERVAL '5' SECOND`。表示 Watermark 总是比观察到的最大事件时间晚 5 秒。
        *   **严格递增 (Strictly Ascending)**: `ts`。假设事件时间戳是严格递增的（没有乱序）。
        *   **单调递增 (Monotonously Increasing)**: `ts - INTERVAL '1' MILLISECOND`。允许时间戳相等，但不允许乱序。
    一旦定义了 Watermark，`<event_time_column>` 就成为了表的**事件时间属性 (rowtime)**。

6.  **`COMMENT table_comment`**: 可选的表注释。

7.  **`PARTITIONED BY (...)`**: 可选地定义表的分区。分区是一种将表数据在物理存储上划分为更小子集的方式，通常基于一个或多个列的值（例如日期、区域）。这对于优化查询性能和管理数据生命周期非常有用。分区列**不能**是计算列或定义 Watermark 的列。分区列的值通常是从文件路径或 Kafka 主题等外部系统中推断出来的，或者在 `INSERT` 语句中指定。

8.  **`WITH (...)`**: 这是 **`CREATE TABLE` 语句的核心部分**，用于配置如何连接到外部系统以及如何解析数据。
    *   **`'connector' = 'connector_identifier'` (必需)**: 指定要使用的连接器。Flink 提供了丰富的内置连接器，例如：
        *   `'filesystem'`: 连接到文件系统（本地、HDFS、S3 等）。
        *   `'kafka'`: 连接到 Apache Kafka。
        *   `'jdbc'`: 连接到关系型数据库。
        *   `'print'`: 用于调试，将数据打印到标准输出/错误。
        *   `'blackhole'`: 用于性能测试，丢弃所有数据。
        *   还有更多针对特定系统的连接器（如 Elasticsearch, HBase, Pulsar 等）。
    *   **其他选项**: 除了 `'connector'`，还需要提供该连接器和所需数据格式所需的其他配置参数。例如：
        *   对于 `'filesystem'` 连接器，可能需要 `'path'`, `'format'`。
        *   对于 `'kafka'` 连接器，需要 `'topic'`, `'properties.bootstrap.servers'`, `'properties.group.id'`, `'scan.startup.mode'`, `'format'` 等。
        *   通用的格式化选项 `'format'`，指定数据的序列化/反序列化方式，如 `'csv'`, `'json'`, `'avro'`, `'parquet'` 等。每种格式也可能有其特定的配置项。
    *   **具体选项请参考 Flink 官方文档中对应连接器和格式的部分。**

**示例：创建一个连接到 Kafka 的表**

```sql
CREATE TABLE user_behavior (
    user_id BIGINT,
    item_id BIGINT,
    category_id BIGINT,
    behavior STRING,
    ts TIMESTAMP(3), -- 事件时间戳
    -- 定义 Watermark，允许 5 秒乱序
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'user_behavior_topic',
    'properties.bootstrap.servers' = 'kafka-broker1:9092,kafka-broker2:9092',
    'properties.group.id' = 'flink_consumer_group',
    'scan.startup.mode' = 'earliest-offset', -- 从最早的 offset 开始消费
    'format' = 'json', -- 数据格式为 JSON
    'json.fail-on-missing-field' = 'false', -- 忽略 JSON 中缺失的字段
    'json.ignore-parse-errors' = 'true' -- 忽略解析错误
);
```

#### ALTER TABLE

`ALTER TABLE` 用于修改现有表的定义，例如重命名表、更改表属性等。

```sql
-- 重命名表
ALTER TABLE [catalog_name.][db_name.]table_name RENAME TO new_table_name;

-- 修改表属性 (WITH 子句中的选项)
ALTER TABLE [catalog_name.][db_name.]table_name SET (key1=val1, key2=val2, ...);
```

*注意：Flink 对 `ALTER TABLE` 的支持有限，例如，目前还不能直接修改列定义、添加/删除列或更改 Watermark 策略。通常需要 `DROP` 然后 `CREATE` 来实现更复杂的结构变更。*

#### DROP TABLE

`DROP TABLE` 用于删除一个已定义的表。

```sql
DROP TABLE [IF EXISTS] [catalog_name.][db_name.]table_name;
```
*   `IF EXISTS` 避免了表不存在时出错。

## 数据操作 (DML): INSERT INTO / INSERT OVERWRITE

虽然本节主要关注 DDL，但与表定义密切相关的是如何向表中写入数据。Flink SQL 使用 `INSERT INTO` 和 `INSERT OVERWRITE` 语句将查询结果写入到目标表中。

```sql
-- 将查询结果追加到目标表 my_sink_table
INSERT INTO my_sink_table
SELECT ... FROM my_source_table ... ;

-- 对于分区表，覆盖指定分区的数据
INSERT OVERWRITE my_partitioned_sink_table PARTITION (dt='2023-10-27', hr='14')
SELECT ... FROM my_source_table WHERE ... ;

-- 对于非分区表或覆盖所有分区 (如果 Sink 支持)
INSERT OVERWRITE my_sink_table
SELECT ... FROM my_source_table ... ;
```

*   `INSERT INTO`: 将查询结果**追加**到目标表中。适用于 Append-only 的 Sink 或支持追加的 Sink。
*   `INSERT OVERWRITE`: **覆盖**目标表或其特定分区的数据。这通常只适用于批处理模式或特定的支持覆盖写操作的 Sink（如文件系统）。对于流处理模式，`INSERT OVERWRITE` 的行为可能受限或不被支持。

这些 `INSERT` 语句也通过 `TableEnvironment.executeSql()` 执行。

通过熟练掌握 Flink SQL DDL，我们可以灵活地定义数据源、数据汇以及数据处理过程中的中间表，为构建复杂的数据管道打下坚实的基础。