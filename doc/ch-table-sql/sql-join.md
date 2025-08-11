(sql-join)=
# Join

:::{note}

本教程已出版为《Flink 原理与实践》，感兴趣的读者请在各大电商平台购买！

<a href="https://item.jd.com/13154364.html"> ![](https://img.shields.io/badge/JD-%E8%B4%AD%E4%B9%B0%E9%93%BE%E6%8E%A5-red) </a>

:::

Join 是 SQL 中用于关联多个表的核心操作。在关系型数据库中，对静态的、有界的数据集进行 Join 操作已经非常成熟。然而，当我们将 Join 应用于流式数据处理时，情况变得更加复杂，因为数据是动态的、持续到达的。

Flink SQL 支持多种 Join 操作，每种都有其特定的语义和适用场景，能够处理流处理中的各种关联需求。本节将介绍 Flink 中主要的流式 Join 类型：常规 Join、时间窗口 Join 和时态表 Join。

## 常规 Join (Regular Join)

在流式数据处理领域，连接（Join）操作是整合来自不同数据源信息的关键手段。Flink SQL 对标准的 SQL Join 语法提供了全面的支持，涵盖了 `INNER JOIN`、`LEFT OUTER JOIN`、`RIGHT OUTER JOIN` 以及 `FULL OUTER JOIN`。这些操作的语法结构与传统的关系型数据库 SQL 标准保持一致，允许开发者便捷地表达数据间的关联逻辑。例如，我们可以通过以下形式将订单流与产品表进行连接：

```sql
SELECT o.order_id, o.order_time, p.product_name, p.category
FROM Orders AS o
JOIN Products AS p ON o.product_id = p.id
WHERE p.category = 'Electronics'; -- 可选的过滤条件
```

然而，与在静态数据集上执行 Join 操作不同，流上的常规 Join 是一种持续演化的计算过程。动态表（Dynamic Table）的概念是理解流式 Join 语义的核心。当参与 Join 的任何一个输入动态表接收到新的数据记录时（对于仅追加流是插入记录，对于 Changelog 流则可能是更新或删除记录），Join 算子会立即根据 `ON` 子句定义的连接条件，在另一张动态表中查找所有匹配的记录。一旦找到匹配项，算子会根据 Join 类型生成相应的变更事件（插入、更新或删除），并将这些结果作为 Changelog 流向下游传递。

具体而言，`INNER JOIN` 仅在连接的两边都能找到匹配记录时才向下游输出连接后的结果。若后续的数据变更（如记录更新或删除）导致原有的匹配关系不再成立，则会相应地生成 `DELETE` 消息。`LEFT OUTER JOIN` 则保证左表的所有记录都会被输出；如果右表存在匹配记录，则合并输出，否则右表对应的字段将填充为 `NULL`。当右表后续有匹配记录到达或发生更新时，会触发 `UPDATE` 消息。`RIGHT OUTER JOIN` 的行为与 `LEFT OUTER JOIN` 对称。而 `FULL OUTER JOIN` 会输出左右两表的所有记录，当某一边缺少匹配记录时，另一边相应的字段会填充为 `NULL`。

在 `flink-book-zh` 项目的示例代码 `RegularJoinExample.java` 中，我们看到了一个典型的常规 Join 应用场景：将实时的 `user_behavior` 流（包含用户ID、物品ID、行为类型和时间戳）与一个相对静态的 `item` 表（包含物品ID和价格）进行 `INNER JOIN`。其目标是实时地为用户的购买行为（`behavior = 'buy'`）关联上对应的商品价格。这个例子清晰地展示了如何定义表、执行 SQL 查询以及处理 Join 结果流。需要注意的是，由于流式 Join 需要维护两边输入流的状态以查找匹配项，因此状态管理成为实现高效、准确流式 Join 的关键挑战之一，尤其是在处理无界数据流时，状态的大小可能会持续增长，需要配合适当的状态TTL（Time-To-Live）策略来控制资源消耗。

### 状态管理

为了防止状态无限增长，对于流上的常规 Join，强烈建议配置状态的 TTL（存活时间）。TTL 定义了状态中的记录可以保留多长时间，一旦记录的空闲时间（没有被访问或更新）超过 TTL，它就会被自动清除。

```java
// 获取 TableEnvironment 的配置
Configuration configuration = tEnv.getConfig().getConfiguration();

// 设置 Join 两侧状态的空闲时间为 1 小时
// 注意：Flink 1.13+ 推荐使用以下方式配置
configuration.set(TableConfigOptions.TABLE_EXEC_STATE_TTL, Duration.ofHours(1));
// 对于更细粒度的控制（例如区分左右流），可能需要查阅具体 Flink 版本的文档

// ... 定义 Orders 和 Products 表 ...

// 执行 Join 查询
Table result = tEnv.sqlQuery("SELECT ... FROM Orders o JOIN Products p ON ... ");
```

通过设置 `table.exec.state.ttl`，我们告诉 Flink 状态中的记录如果空闲超过 1 小时，就应该被清理掉。这可以有效控制状态大小，但也意味着超过 TTL 到达的匹配记录将无法产生连接结果。TTL 的时长需要根据业务逻辑和数据特性来权衡：太短可能导致本应匹配的数据因提前过期而无法 Join；太长则可能导致状态过大。

:::danger
在生产环境中使用无时间限制的常规 Join 时，必须配置状态 TTL，以防止状态无限增长导致系统不稳定。
:::

## 时间窗口 Join (Time-windowed Join)

时间窗口 Join 主要用于关联两个数据流中在时间上非常接近的事件。它要求 Join 条件中必须包含一个时间约束，限制两个事件的时间戳（通常是事件时间）差异在某个范围内。这与我们在第五章讨论的 DataStream API 中的 Interval Join 非常相似。

### 场景示例

假设我们有两个数据流：

- Impressions: 广告展示流 (ad_id, impression_ts)
- Clicks: 广告点击流 (ad_id, click_ts)

我们想将发生在广告展示后 1 小时内的点击与对应的展示关联起来。

### 语法

时间窗口 Join 通过在 ON 子句中添加时间边界谓词来实现。

```sql
SELECT
    i.ad_id,
    i.impression_ts,
    c.click_ts
FROM Impressions AS i JOIN Clicks AS c
  ON i.ad_id = c.ad_id -- Join Key 条件
 AND c.click_ts BETWEEN i.impression_ts AND i.impression_ts + INTERVAL '1' HOUR; -- 时间窗口条件
```

`BETWEEN` 条件定义了点击时间 (`c.click_ts`) 必须落在 `[i.impression_ts, i.impression_ts + 1 hour]` 这个区间内。也可以使用比较运算符：

```sql
SELECT ...
FROM table1 t1 JOIN table2 t2
  ON t1.key = t2.key
 AND t1.time_attr >= t2.time_attr - INTERVAL '5' MINUTE -- t1 时间不早于 t2 时间减 5 分钟
 AND t1.time_attr <  t2.time_attr + INTERVAL '10' MINUTE; -- t1 时间严格早于 t2 时间加 10 分钟
```

### 工作原理与状态管理

与常规 Join 不同，时间窗口 Join 的状态是有界的。Flink 只需要为每个输入流保留其时间窗口范围内的记录。例如，在广告点击的例子中：

1. 当一条 Impression 记录到达时，它会被保存在状态中，等待 1 小时。如果 1 小时内没有匹配的 Click 到达，或者 Watermark 超过了 `impression_ts + 1 hour`，这条 Impression 记录就可以从状态中安全地移除了。
2. 当一条 Click 记录到达时，Flink 会在状态中查找过去 1 小时内到达的、具有相同 `ad_id` 的 Impression 记录进行连接。

由于时间窗口限制，状态的大小是可控的，通常不需要像常规 Join 那样强制配置 TTL（尽管配置 TTL 仍然是最佳实践）。

:::info
时间窗口 Join 通常用于 Append-only 的流（即只有插入操作）。
:::

## 时态表 Join (Temporal Table Join)

时态表 Join 用于将一个流（通常是事实流）与一个随时间变化的版本化表（通常是维度表）进行关联。核心思想是：对于流中的每一条记录，根据其自身的时间戳，去查找版本化表中在那个特定时间点有效的记录。

### 场景示例

Orders 流: 包含订单信息 (order_id, product_id, order_time)
Products 表: 包含产品信息，但产品的价格 (price) 会随时间变化。这是一个版本化表，通常包含 product_id, price, valid_start_time, valid_end_time 或者一个表示变更时间的字段 change_time。
我们希望为每个订单关联其下单时对应的产品价格。

### 定义时态表 (Temporal Table)

首先，需要将版本化的 Products 表定义为一个时态表。这通常通过在 CREATE TABLE 语句中声明主键（PRIMARY KEY）和事件时间属性（EVENT TIME），并启用版本化。

```sql
-- 假设 Products_History 表存储了产品价格的所有历史变更记录
CREATE TABLE Products_History (
    product_id STRING,
    name STRING,
    price DECIMAL(10, 2),
    change_time TIMESTAMP(3), -- 价格生效的时间
    WATERMARK FOR change_time AS change_time - INTERVAL '1' SECOND, -- 定义 Watermark
    PRIMARY KEY (product_id) NOT ENFORCED -- 定义主键，对于版本化很重要
) WITH (
    'connector' = 'kafka',
    'topic' = 'products_changelog',
    'properties.bootstrap.servers' = '...',
    'format' = 'json',
    ...
);

-- Flink 可以基于 Products_History 自动推断出版本化视图
```

### 时态 Join 语法 (SQL:2011 标准)

Flink 推荐使用 SQL:2011 标准的 FOR SYSTEM_TIME AS OF 语法来执行时态 Join：

```sql
SELECT
    o.order_id,
    o.order_time,
    p.name,
    p.price -- 获取订单发生时产品的价格
FROM
    Orders AS o
JOIN
    -- 指定要关联的时态表 Products_History
    -- 并指定根据 Orders 表的 order_time 来查找版本
    Products_History FOR SYSTEM_TIME AS OF o.order_time AS p
ON
    o.product_id = p.product_id; -- Join 条件
```

### 解释

Products_History FOR SYSTEM_TIME AS OF o.order_time AS p: 这部分是时态 Join 的核心。它告诉 Flink：

1. 我们要关联 Products_History 表。
2. 对于 Orders 流中的每一条记录 o，我们需要查找 Products_History 表中，在 o.order_time 这个时间点有效的版本。
3. 将找到的版本别名为 p。
ON o.product_id = p.product_id: 这是普通的 Join 条件，用于匹配订单和产品。

### 工作原理与状态管理

Flink 会为版本化表 Products_History 维护状态。当 Orders 流中的记录到达时，Flink 会根据 o.order_time 和 o.product_id 在状态中查找对应的产品版本。

如果版本化表是基于 Changelog（如来自 Debezium 的 CDC 数据或带有时间戳的价格更新流），Flink 会有效地维护每个 Key 的最新版本和历史版本（直到状态 TTL 清理）。
状态管理通常比无限制的常规 Join 更有效，因为查找是基于时间的点查询，并且可以通过 TTL 控制历史版本的保留时间。

:::info
时态 Join 是处理流数据与动态维度数据关联的强大工具，在实时数仓、风控、推荐等场景中应用广泛。
:::

## 总结

Flink SQL 提供了灵活的 Join 机制来处理流式数据：

- 常规 Join: 功能最全，但需要仔细管理状态 TTL 以防无限增长。
- 时间窗口 Join: 适用于关联时间上接近的事件，状态有界。
- 时态表 Join: 用于流数据与版本化表的关联，实现时间点查找，推荐使用 FOR SYSTEM_TIME AS OF 语法。
根据具体的业务需求和数据特性选择合适的 Join 类型，并理解其状态管理机制，是有效使用 Flink SQL 的关键。