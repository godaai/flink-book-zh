(sql-ddl)=
# SQL DDL

:::{note}

本教程已出版为《Flink 原理与实践》，感兴趣的读者请在各大电商平台购买！

<a href="https://item.jd.com/13154364.html"> ![](https://img.shields.io/badge/JD-%E8%B4%AD%E4%B9%B0%E9%93%BE%E6%8E%A5-red) </a>


:::

通过前面的一些介绍，我们已经对 Flink SQL 有了一些了解。Flink SQL 底层使用 Dynamic Table 来进行流处理，它支持了时间窗口和 Join 操作。本节将围绕 SQL DDL，主要介绍创建、获取、修改和删除元数据时所涉及的一些注意事项。

## 注册和获取表

### Catalog 简介

Catalog 记录并管理了各类元数据信息，比如我们有哪些数据库（Database）、数据库是以文件还是消息队列的形式存储、某个数据库中有哪些表和视图、当前有哪些可用的函数等。Catalog 提供了一个注册、管理和访问这些元数据的 API。一般情况下，一个 Catalog 中包含一到多个 Database，一个 Database 包含一到多个 Table。

常见的 Catalog 包括：`GenericInMemoryCatalog`、`HiveCatalog` 和用户自定义的 Catalog。

`GenericInMemoryCatalog` 将元数据存储在内存中，只在一个 Session 会话内生效。默认情况下，都是使用这种 Catalog。

Hive 的元数据管理功能是 SQL-on-Hadoop 领域事实上的标准，很多企业的生产环境使用 Hive 管理元数据，Hive 可以管理包括纯 Hive 表和非 Hive 表。为了与 Hive 生态兼容，Flink 推出了 `HiveCatalog`。对于同时部署了 Hive 和 Flink 的环境，`HiveCatalog` 允许用户在 Flink SQL 上读取原来 Hive 中的各个表。对于只部署了 Flink 的环境，`HiveCatalog` 是目前将元数据持久化的唯一方式。持久化意味着某些元数据管理团队可以先将 Kafka 或 HDFS 中的数据注册到 Catalog 中，其他数据分析团队无需再次注册表，只需每次从 Catalog 中获取表，不用关心数据管理相关问题。如果没有进行元数据持久化，用户每次都需要注册表。

用户也可以自定义 Catalog，需要实现 `Catalog` 接口类。

### 获取表

我们编写好 SQL 查询语句（`SELECT ...` 语句）后，需要使用 `TableEnvironment.sqlQuery("SELECT ...")` 来执行这个 SQL 语句，这个方法返回的结果是一个 `Table`。`Table` 可以用于后续的 Table API & SQL 查询，也可以将 `Table` 转换为 `DataStream` 或者 `DataSet`。总之，Table API 和 SQL 可以完美融合。

我们使用 `FROM table_name` 从某个表中查询数据，这个 `table_name` 表必须被注册到 `TableEnvironment` 中。注册有以下几种方式：

* 使用 SQL DDL 中的 `CREATE TABLE ...` 创建表
* `TableEnvironment.connect()` 连接一个外部系统
* 从 Catalog 中获取已注册的表

第一种 SQL DDL 的方式，我们会用 `CREATE TABLE table_name ...` 来明确指定一个表的名字为 `table_name`。而第二种和第三种方式，我们是在 Java/Scala 代码中获取一个 `Table` 对象，获取对象后，明确使用 `createTemporaryView()` 方法声明一个 `Table` 并指定一个名字。例如，下面的代码中，我们获取了一个 `Table`，并通过 `createTemporaryView()` 注册该 `Table` 名字为 `user_behavior`。

```java
Table userBehaviorTable = tEnv.fromDataStream(userBehaviorStream, "user_id, item_id, behavior, ts.rowtime");

// createTemporaryView 创建名为 user_behavior 的表
tEnv.createTemporaryView("user_behavior", userBehaviorTable);
```

对于 `TableEnvironment.connect()` 所连接的外部系统，也可以使用 `createTemporaryTable` 方法，以链式调用的方式注册名字：

```java
tEnv
  // 使用 connect 函数连接外部系统
  .connect(...)
  // 序列化方式 可以是 JSON、Avro 等
  .withFormat(...)
  // 数据的 Schema
  .withSchema(...)
  // 临时表的表名，后续可以在 SQL 语句中使用这个表名
  .createTemporaryTable("user_behavior");
```

注册好表名后，就可以在 SQL 语句中使用 `FROM user_behavior` 来查询该表。

即使没有明确指定表名，也没有问题。`Table.toString()` 可以返回表的名字，如果没有给这个 `Table` 指定名字，Flink 会为其自动分配一个唯一的表名，不会与其他表名冲突。如下所示：

```java
// 获取一个 Table，并没有明确为其分配表名
Table table = ...;
String tableName = table.toString();
tEnv.sqlQuery("SELECT * FROM " + tableName);
```

甚至可以省略 `toString` 方法，直接将 `Table` 对象与 SQL 语句用加号 `+` 连接，因为 `+` 会自动调用 `Table.toString()` 方法。

```java
// 获取一个 Table，并没有明确为其分配表名
Table table = ...;
// 加号 + 操作符会在编译期间自动调用 Table.toString() 方法
tEnv.sqlQuery("SELECT * FROM " + table);
```

### HiveCatalog

如果想将元数据持久化到 `HiveCatalog` 中：

```java
TableEnvironment tEnv = ...

// 创建一个 HiveCatalog
// 四个参数分别为：catalogName、databaseName、hiveConfDir、hiveVersion
Catalog catalog = new HiveCatalog("mycatalog", null, "<path_of_hive_conf>", "<hive_version>");

// 注册 catalog，取名为 mycatalog
tEnv.registerCatalog("mycatalog", catalog);

// 创建一个 Database，取名为 mydb
tEnv.sqlUpdate("CREATE DATABASE mydb WITH (...)");

// 创建一个 Table，取名为 mytable
tEnv.sqlUpdate("CREATE TABLE mytable (name STRING, age INT) WITH (...)");

// 返回所有 Table
tEnv.listTables(); 
```

## USE 和 SHOW 语句

创建完 Catalog、Database 后，可以像其他 SQL 引擎一样，使用 `SHOW` 和 `USE` 语句：

```sql
-- 展示所有 Catalog
SHOW CATALOGS;

-- 使用 mycatalog
USE CATALOG mycatalog;

-- 展示当前 Catalog 里所有 Database
SHOW DATABASES;

-- 使用 mydatabase
USE mydb;

-- 展示当前 Catalog 里当前 Database 里所有 Table
SHOW TABLES;
```

这些语句需要粘贴到 `sqlUpdate` 方法中：

```java
Catalog catalog = ...

tEnv.registerCatalog("mycatalog", catalog);
tEnv.sqlUpdate("USE catalog mycatalog");
tEnv.sqlUpdate("CREATE DATABASE mydb");
tEnv.sqlUpdate("USE mydb");
```

## CREATE、DROP、ALTER

`CREATE`、`ALTER`、`DROP` 是 SQL 中最常见的三种 DDL 语句，可以创建、修改和删除数据库（Database）、表（Table）和函数（Function）。

`CREATE` 语句可以创建一个 Database、Table 和 Function：

* `CREATE TABLE`
* `CREATE DATABASE`
* `CREATE FNCTION`

`ALTER` 语句可以修改已有的 Database、Table 和 Function：

* `ALTER TABLE`
* `ALTER DATABASE`
* `ALTER FNCTION`

`DROP` 语句可以删除之前创建的 Database、Table 和 Function：

* `DROP TABLE`
* `DROP DATABASE`
* `DROP FUNCTION`

这些语句可以放到 `TableEnvironment.sqlUpdate()` 的参数里，也可以在 SQL Client 里执行。`TableEnvironment.sqlUpdate()` 执行成功后没有返回结果，执行失败则会抛出异常。下面例子展示了如何在 `TableEnvironment` 中使用 `sqlUpdate`：

```java
// tEnv：TableEnvironment
// CREATE TABLE
tEnv.sqlUpdate("CREATE TABLE user_behavior (" +
                "    user_id BIGINT," +
                "    item_id BIGINT," +
                "    category_id BIGINT," +
                "    behavior STRING," +
                "    ts TIMESTAMP(3)," +
                "    WATERMARK FOR ts as ts - INTERVAL '5' SECOND  -- 在ts上定义watermark，ts成为事件时间列" +
                ") ...");

// ALTER DATABASE
tEnv.sqlUpdate("ALTER DATABASE db1 set ('k1' = 'a', 'k2' = 'b')")

// DROP TABLE
tEnv.sqlUpdate("DROP TABLE user_behavior");
```

### CREATE/ALTER/DROP TABLE

#### CREATE TABLE

`CREATE TABLE` 需要按照下面的模板编写：

```sql
CREATE TABLE [catalog_name.][db_name.]table_name
  (
    {<column_definition> | <computed_column_definition>}[, ...n]
    [<watermark_definition>]
  )
  [COMMENT table_comment]
  [PARTITIONED BY (partition_column_name1, partition_column_name2, ...)]
  WITH (key1=val1, key2=val2, ...)
```

一个名为 `table_name` 的 `Table` 隶属于一个名为 `db_name` 的 Database，`db_name` 又隶属于名为 `catalog_name` 的 Catalog。如果不明确指定 `db_name` 和 `catalog_name`，该表被注册到默认的 Catalog 和默认的 Database 中。如果 `table_name` 与已有的表名重复，会抛出异常。

`<column_definition>`、`<computed_column_definition>` 和 `<watermark_definition>` 需要符合下面的模板：

```sql
<column_definition>:
  column_name column_type [COMMENT column_comment]

<computed_column_definition>:
  column_name AS computed_column_expression [COMMENT column_comment]

<watermark_definition>:
  WATERMARK FOR rowtime_column_name AS watermark_strategy_expression
```

`COMMENT` 用来对字段做注释，使用 `DESCRIBE table_name` 命令时，可以查看到字段的一些注释和描述信息。

`<column_definition>` 在传统的 SQL DDL 中经常见到，Watermark 策略在 [窗口](sql-window) 部分已经介绍，不再赘述，这里主要介绍计算列（Computed Column）。

#### 计算列

计算列是虚拟字段，不是一个实际存储在表中的字段。计算列可以通过表达式、内置函数、或是自定义函数等方式，使用其它列的数据，计算出其该列的数值。比如，一个订单表中有单价 `price` 和数量 `quantity`，总价可以被定义为 `total AS price * quantity`。计算列表达式可以是已有的物理列、常量、函数的组合，但不能是 `SELECT ...` 式的子查询。计算列虽然是个虚拟字段，但在 Flink SQL 中可以像普通字段一样被使用。

计算列常常被用于定义时间属性，在 [窗口](sql-window) 部分我们曾介绍了如何定义 Processing Time：使用 `proc AS PROCTIME()` 来定义一个名为 `proc` 的计算列，该列可以在后续计算中被用做时间属性。`PROCTIME()` 是 Flink 提供的内置函数，用来生成 Processing Time。

```sql
CREATE TABLE mytable (
    id BIGINT,
    -- 在原有 Schema 基础上添加一列 proc
    proc as PROCTIME()
) WITH (
    ...
);
```

关于计算列，相关知识点可以总结为：

* 计算列是一个虚拟列，计算的过程可以由函数、已有物理列、常量等组成，可以用在 `SELECT` 语句中。
* 计算列不能作为 `INSERT` 语句的输出目的地，或者说我们不能使用 `INSERT` 语句将数据插入到目标表的计算列上。

#### WITH

在 [Table API & SQL 简介](table-overview.md) 中我们曾介绍连接外部系统时必须配置相应参数，这些参数以 `key=value` 等形式被放在 `WITH` 语句中。

```sql
CREATE TABLE my_table (
  -- Schema
  ...
) WITH (
  -- 声明参数
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  ...
)
```

在这个例子中使用的外部系统是 Kafka，我们需要配置一些 Kafka 连接相关的参数。Flink 的官方文档中有不同 Connector 的详细参数配置示例，这里不再详细介绍每种 Connector 需要配置哪些参数，

使用 `CREATE TABLE` 创建的表可以作为 Source 也可以作为 Sink。

#### PARTITIONED BY

根据某个字段进行分区。如果这个表中的数据实际存储在一个文件系统上，Flink 会为每个分区创建一个文件夹。例如，以日期为分区，那么每天的数据被放在一个文件夹中。`PARTITIONED BY` 经常被用在批处理中。

:::note
这里是 `PARTITIONED BY`，与 `OVER WINDOW` 中的 `PARTITON BY` 语法和含义均不同。
:::

#### ALTER TABLE

`ALTER TABLE` 目前支持修改表名和一些参数。

```sql
-- 修改表名
ALTER TABLE [catalog_name.][db_name.]table_name RENAME TO new_table_name

-- 修改参数，如果某个 Key 之前被 WITH 语句设置过，再次设置会将老数据覆盖
ALTER TABLE [catalog_name.][db_name.]table_name SET (key1=val1, key2=val2, ...)
```

#### DROP TABLE

`DROP TABLE` 用来删除一个表。`IF EXISTS` 表示只对已经存在的表进行删除。

```sql
DROP TABLE [IF EXISTS] [catalog_name.][db_name.]table_name
```

### CREATE/ALTER/DROP DATABASE

#### CREATE DATABASE

`CREATE DATABASE` 一般使用下面的模板：

```sql
CREATE DATABASE [IF NOT EXISTS] [catalog_name.]db_name
  [COMMENT database_comment]
  WITH (key1=val1, key2=val2, ...)
```

这里的语法与其他 SQL 引擎比较相似。其中 `IF NOT EXISTS` 表示，如果这个 Database 不存在才创建，否则不会创建。

#### ALTER DATABASE

```sql
ALTER DATABASE [catalog_name.]db_name SET (key1=val1, key2=val2, ...)
```

`ALTER DATABASE` 支持修改参数，新数据会覆盖老数据。

#### DROP DATABASE

```sql
DROP DATABASE [IF EXISTS] [catalog_name.]db_name [(RESTRICT | CASCADE) ]
```

`RESTRICT` 选项表示如果 Database 非空，那么会抛出异常，默认开启本选项；`CASCADE` 选项表示会将 Database 下所属的 Table 和 Function 等都删除。

### CREATE/ALTER/DROP FUNCTION

除了传统 SQL 引擎都会提供的上述 `CREATE` 功能，Table API & SQL 还提供了函数（Function）功能。我们也可以用 Java 或 Scala 语言自定义一个函数，然后注册进来，在 SQL 语句中使用。`CREATE FUNCTION` 的模板如下所示，我们将在 [用户自定义函数](catalog-function.md) 部分的详细介绍如何使用 Java/Scala 自定义函数。

```sql
CREATE [TEMPORARY|TEMPORARY SYSTEM] FUNCTION 
  [IF NOT EXISTS] [catalog_name.][db_name.]function_name 
  AS identifier [LANGUAGE JAVA|SCALA]
```

## INSERT

`INSERT` 语句可以向表中插入数据，一般用于向外部系统输出数据。它的语法模板如下：

```sql
INSERT {INTO | OVERWRITE} [catalog_name.][db_name.]table_name [PARTITION part_spec] 
SELECT ...

part_spec:
  (part_col_name1=val1 [, part_col_name2=val2, ...])
```

上面的 SQL 语句将 `SELECT` 查询结果写入到目标表中。`OVERWRITE` 选项表示将原来的数据覆盖，否则新数据只是追加进去。`PARTITION` 表示数据将写入哪个分区。如果 `CREATE TABLE` 是按照日期进行 `PARTITIONED BY` 分区，那么每个日期会有一个文件夹，`PARTITION` 后要填入一个日期，数据将写入日期对应目录中。分区一般常用于批处理场景。

```sql
-- 创建一个表，用来做输出
CREATE TABLE behavior_cnt (
  user_id BIGINT,
  cnt BIGINT
) WITH (
  'connector.type' = 'filesystem',  -- 使用 filesystem connector
  'connector.path' = 'file:///tmp/behavior_cnt',  -- 输出地址为一个本地路径
  'format.type' = 'csv'  -- 数据源格式为 json
)

-- 向一个 Append-only 表中输出数据
INSERT INTO behavior_cnt 
SELECT 
	user_id, 
	COUNT(behavior) AS cnt 
FROM user_behavior 
GROUP BY user_id, TUMBLE(ts, INTERVAL '10' SECOND)
```

或者是将特定的值写入目标表：

```sql
INSERT {INTO | OVERWRITE} [catalog_name.][db_name.]table_name VALUES values_row [, values_row ...]
```

例如：

```sql
CREATE TABLE students (name STRING, age INT, score DECIMAL(3, 2)) WITH (...);

INSERT INTO students
  VALUES ('Li Lei', 35, 1.28), ('Han Meimei', 32, 2.32);
```