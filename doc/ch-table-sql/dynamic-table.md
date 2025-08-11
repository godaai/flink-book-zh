(dynamic-table)=
# 动态表与持续查询

:::{note}

本教程已出版为《Flink 原理与实践》，感兴趣的读者请在各大电商平台购买！

<a href="https://item.jd.com/13154364.html"> ![](https://img.shields.io/badge/JD-%E8%B4%AD%E4%B9%B0%E9%93%BE%E6%8E%A5-red) </a>


:::

在上一节中，我们初步领略了 Flink Table API & SQL 的强大功能，了解了如何定义表、执行查询。然而，当我们将这些熟悉的关系型操作应用于**流式数据**时，会遇到一些与传统批处理截然不同的挑战和概念。本节，我们将深入探讨 Flink 如何通过**动态表（Dynamic Table）**和**持续查询（Continuous Query）**这两个核心概念，优雅地在无界的数据流上实现关系型查询。

## 理解流上的关系查询：动态表与持续查询

关系型数据库及其查询语言 SQL，自上世纪 70 年代诞生以来，已成为数据处理领域的基石。它们基于关系代数，天然适合处理**有界、静态**的数据集——我们称之为**批处理**。在批处理模式下，一次查询针对的是一个完整且不变的数据快照，执行过程有明确的起点和终点，最终产生一个确定的结果。

然而，现代应用常常需要处理的是**无界、持续不断**的数据流，例如用户点击流、传感器读数、交易日志等。直接将为批处理设计的 SQL 应用于这种场景，会遇到根本性的问题：

| 特性       | 批处理关系型查询 / SQL                      | 流处理                                 |
| ---------- | :------------------------------------------: | :-------------------------------------: |
| 输入数据   | **有界**，查询作用于一个完整的数据集。          | **无界**，数据持续不断地流入。           |
| 执行过程   | 查询在**静态**数据上执行一次，有始有终。      | 查询**持续运行**，不断处理新到达的数据。   |
| 查询结果   | 查询完成后生成一个**最终、确定**的结果。       | 查询结果会随着新数据的到来而**持续更新**。 |

面对这些挑战，Flink 引入了**动态表（Dynamic Table）**的概念。动态表不是一个静态的数据快照，而是一个**随时间演变的表**。它逻辑上代表了一个数据流，随着流中新数据的到达，动态表的内容也在不断地**变化**。你可以将动态表想象成数据库中**物化视图（Materialized View）**的一种流式模拟：当底层数据（即数据流）发生变化时，这个“视图”（动态表）也会相应更新。

在动态表上执行的查询，我们称之为**持续查询（Continuous Query）**。与批处理查询一次性产生结果不同，持续查询会一直运行，并根据输入动态表的变化，**持续地更新**其输出结果。这个输出结果本身，也是一个**动态表**。

```{figure} ./img/dynamic-table.png
---
name: fig-dynamic-table
width: 80%
align: center
---
动态表（Dynamic Table）与持续查询（Continuous Query）的关系
```

{numref}`fig-dynamic-table` 清晰地展示了这个过程：

1.  **数据流 (Stream)** 被解释（或转换）为一个**动态表 (Dynamic Table)**。
2.  在这个动态表上执行一个**持续查询 (Continuous Query)**。
3.  持续查询产生的结果是一个新的**动态表 (Dynamic Table)**。
4.  这个结果动态表可以被转换回一个**数据流 (Stream)**，用于后续处理或输出到外部系统。

### 示例：用户行为分析

让我们通过熟悉的电商用户行为数据流，来直观感受动态表和持续查询的工作方式。

```{figure} ./img/data-to-table.png
---
name: fig-data-to-table
width: 80%
align: center
---
用户行为数据流转换为动态表
```

如 {numref}`fig-data-to-table` 所示，左侧是持续流入的用户行为事件流（包含用户 ID、行为类型、时间戳）。这个流可以被 Flink Table API & SQL 解释为右侧的 `user_behavior` 动态表。请注意，这个表的内容不是固定的，它会随着新事件的到来而增长。

现在，假设我们在这个 `user_behavior` 动态表上执行一个持续查询，统计每个用户的行为总数：

```sql
SELECT
	user_id,
	COUNT(behavior) AS behavior_cnt
FROM user_behavior
GROUP BY user_id
```

这个 SQL 查询本身看起来和批处理中的聚合查询没什么两样。但在流处理的背景下，它的执行过程是**持续**的，并且其结果（也是一个动态表）会**不断更新**。

```{figure} ./img/continuous-query.png
---
name: fig-continuous-query
width: 80%
align: center
---
持续查询示例：统计用户行为总数及其结果的演变
```

{numref}`fig-continuous-query` 展示了查询结果动态表随时间演变的过程：

-   当第一条数据 `(1, pv, 00:00:00)` 到达时，输入动态表更新，触发查询计算。结果动态表中插入 `(1, 1)`。
-   当第二条数据 `(2, fav, 00:00:00)` 到达时，输入动态表再次更新。结果动态表中插入 `(2, 1)`。
-   当第三条数据 `(1, cart, 00:00:02)` 到达时，输入动态表更新。这次，用户 `1` 已经存在于结果中。查询引擎需要**更新**该用户的计数值。结果动态表中的 `(1, 1)` 被更新为 `(1, 2)`。
-   这个过程会随着新数据的不断流入而持续进行。

### 考虑时间：窗口聚合

上面的查询统计的是用户的**历史总行为数**。如果我们只关心某个**时间段**内的统计，比如每分钟的用户行为数，就需要引入**窗口**的概念。我们将在后续章节 ([窗口](sql-window)) 详细讨论窗口，这里先看一个简单的例子：

```sql
SELECT
	user_id,
	COUNT(behavior) AS behavior_cnt,
	TUMBLE_END(ts, INTERVAL '1' MINUTE) AS end_ts -- 计算所属滚动窗口的结束时间
FROM user_behavior
GROUP BY
    user_id,
    TUMBLE(ts, INTERVAL '1' MINUTE) -- 按用户ID和1分钟滚动窗口分组
```

这个查询引入了 `TUMBLE` 函数，将数据流按时间（`ts` 字段）划分成连续不重叠的 1 分钟窗口，并在每个窗口内独立地按 `user_id` 进行聚合。

```{figure} ./img/tumble-append.png
---
name: fig-tumble-append
width: 80%
align: center
---
持续查询示例：统计用户在一分钟窗口内的行为
```

{numref}`fig-tumble-append` 展示了这个窗口聚合查询的执行过程。

-   对于 `00:00:00` 到 `00:00:59` 这个窗口，查询处理该时间段内的 5 条数据，计算出每个用户的行为数，并将结果（包含窗口结束时间 `00:01:00`）输出到结果动态表中。
-   当时间进入下一个窗口（`00:01:00` 到 `00:01:59`），查询开始处理这个新窗口的数据。这个窗口的计算结果（例如 `(1, 1, 00:02:00)`）会被**追加**到结果动态表中，而不会影响上一个窗口的结果。

### 结果更新模式：Append vs. Update

通过对比上述两个查询示例，我们观察到持续查询产生结果动态表的方式有所不同：

1.  **窗口聚合查询**：每个窗口的结果一旦计算完成就不会再改变。新的结果总是以**追加（Append）**的方式添加到结果动态表中。这种查询被称为**仅追加查询（Append-only Query）**。
2.  **普通聚合查询（无窗口）**：随着新数据的到来，已有的聚合结果（如用户 `1` 的计数）可能会被**更新（Update）**。这种查询的结果动态表既包含插入（`INSERT`）操作，也包含更新（`UPDATE`）或删除（`DELETE`）操作。

这种差异对于如何将结果动态表转换回数据流，或者如何将结果写入外部系统（Sink）至关重要。

## 动态表的输出：转换回数据流

当我们需要将持续查询的结果（一个动态表）发送给下游 Flink 算子（作为 DataStream）或写入外部系统时，就需要考虑如何处理结果动态表中的变化。Flink 提供了几种模式来将动态表转换回数据流：

1.  **追加流（Append-only Stream）**：
    *   **适用场景**：用于仅包含 `INSERT` 操作的动态表（例如，由仅追加查询产生，或简单的 `SELECT...WHERE...` 查询）。
    *   **工作方式**：动态表中的每一行新数据被转换成数据流中的一个元素，直接追加到流中。
    *   **API**：`tableEnv.toAppendStream(table, T.class)`

    ```java
    StreamTableEnvironment tEnv = ...
    Table appendOnlyTable = ... // 一个由仅追加查询产生的Table

    // 将 appendOnlyTable 转换为 DataStream<Row>
    // 每一行新结果都作为一个 Row 对象发送到流中
    DataStream<Row> dsRow = tEnv.toAppendStream(appendOnlyTable, Row.class);
    ```

2.  **撤回流（Retract Stream）**：
    *   **适用场景**：用于包含 `INSERT`、`UPDATE` 和 `DELETE` 操作的动态表（例如，由包含聚合、`GROUP BY`、`JOIN` 等操作的查询产生）。
    *   **工作方式**：`UPDATE` 操作被编码为对旧行的**撤回（Retract）**消息和对新行的**添加（Add）**消息。`DELETE` 操作被编码为撤回消息。
    *   **API**：`tableEnv.toRetractStream(table, T.class)`
    *   **输出格式**：流中的元素通常是一个二元组 `Tuple2<Boolean, T>`，其中 `Boolean` 标志位表示这条记录是添加 (`true`) 还是撤回 (`false`)，`T` 是实际的数据类型（如 `Row`）。

    ```{figure} ./img/retract.png
    ---
    name: fig-retract-mode
    width: 80%
    align: center
    ---
    Retract 模式下对数据更新的表示
    ```

    如 {numref}`fig-retract-mode` 所示（对应第一个无窗口聚合查询），当用户 `1` 的计数从 `1` 变为 `2` 时：
    *   首先发送一条撤回消息：`(false, (1, 1))`，表示旧的结果 `(1, 1)` 无效了。
    *   然后发送一条添加消息：`(true, (1, 2))`，表示新的结果是 `(1, 2)`。

    ```java
    StreamTableEnvironment tEnv = ...
    Table updatingTable = ... // 一个结果会更新的Table

    // 将 updatingTable 转换为 DataStream<Tuple2<Boolean, Row>>
    // Boolean 标志位表示是添加(true)还是撤回(false)
    DataStream<Tuple2<Boolean, Row>> retractStream = tEnv.toRetractStream(updatingTable, Row.class);
    ```

3.  **更新插入流（Upsert Stream）**：
    *   **适用场景**：与 Retract 流类似，用于包含更新操作的动态表。但有一个**前提条件**：结果动态表必须有一个**唯一的键（Unique Key）**，并且下游的 Sink 能够根据这个键处理 `UPDATE` 或 `DELETE` 操作（例如，写入支持按键更新的数据库）。
    *   **工作方式**：`UPDATE` 操作直接被编码为更新消息，`DELETE` 操作被编码为删除消息。相比 Retract 模式，它可能更高效，因为它不发送额外的撤回消息。
    *   **API**：通常不直接通过 `toUpsertStream` 这样的通用 API（虽然 Flink 内部有此概念），而是与特定的支持 Upsert 的 `TableSink`（在较新 Flink 版本中是 `DynamicTableSink`）结合使用。连接器（Connector）会声明它支持 Upsert 模式，并要求在定义表时指定主键。
    *   **示例**：将结果写入 MySQL 或 HBase 等支持主键更新的系统时，通常会利用 Upsert 模式。

选择哪种输出模式取决于持续查询的类型以及下游 Sink 的能力。

## 流处理的挑战与状态管理

虽然动态表和持续查询为流上关系查询提供了强大的模型，但在实践中也存在一些挑战，主要与**状态管理**有关。

持续查询（尤其是包含聚合、`JOIN` 等操作的查询）通常需要在 Flink 内部维护**状态（State）**来存储中间结果或必要的信息（例如，`GROUP BY` 查询需要存储每个分组的当前聚合值）。

-   **状态大小**：对于无界流，如果不加限制，状态可能会无限增长。例如，在第一个用户行为统计查询中，如果不断有新的 `user_id` 出现，存储 `(user_id, behavior_cnt)` 的状态会持续增大，最终可能耗尽内存资源，导致作业失败。
-   **更新成本**：某些 SQL 操作（如需要全局排序的 `RANK()` 或 `ORDER BY`）在流上的计算成本非常高。每当新数据到来，可能需要重新计算和排序大量已有状态，这对性能是巨大的考验。Flink 正在不断优化对这类操作的支持，但用户在设计查询时仍需考虑其潜在开销。

为了应对状态无限增长的问题，Flink 提供了**状态生存时间（Time-To-Live, TTL）**机制，允许为作业状态配置空闲时间，自动清理长时间未更新的状态条目。

```java
StreamTableEnvironment tEnv = ...
TableEnvironmentSettings settings = ...
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
tEnv = StreamTableEnvironment.create(env, settings);

// 获取 Table 配置
Configuration configuration = tEnv.getConfig().getConfiguration();

// 设置空闲状态保留时间：最短1小时，最长2小时
// 注意：这是旧版API，新版推荐在Table API/SQL DDL中配置
// configuration.setString("table.exec.state.ttl", "1 h"); // 示例：设置一个统一的TTL，新版配置方式可能不同
// 或者使用旧的API（可能已废弃或行为变更）:
tEnv.getConfig().setIdleStateRetentionTime(Time.hours(1), Time.hours(2));

```

**（注意：上面 `setIdleStateRetentionTime` 是较早版本的 API，在 Flink 1.11 及之后版本，推荐使用 SQL DDL 或 `TableConfig` 中更细粒度的 TTL 配置。具体配置方式请参考所使用 Flink 版本的官方文档。）**

`setIdleStateRetentionTime(minTime, maxTime)` 方法（或等效的新配置）定义了状态的空闲保留时间范围：

-   `minTime`：状态条目至少会保留这么长时间，即使它一直处于空闲状态（没有被访问或更新）。
-   `maxTime`：状态条目如果空闲时间超过这个阈值，就**可能**会被 Flink 清理掉。

**重要影响**：一旦某个状态条目（例如某个 `user_id` 的计数）因为空闲而被清理，当后续该 `user_id` 的数据再次到来时，Flink 会将其视为一个全新的 `user_id`，从初始状态开始计算。这意味着启用状态 TTL 后，对于某些查询（特别是没有使用时间窗口的聚合），得到的结果可能是**近似准确**的。这是在**系统资源消耗**和**结果绝对精确度**之间的一种权衡。如果要求结果绝对精确，则不应配置 TTL（或确保 TTL 足够长以覆盖业务逻辑所需的时间范围），并需要仔细评估状态大小和资源需求。

将 `minTime` 和 `maxTime` 都设置为 0（或不配置 TTL）表示状态永不自动过期。

理解动态表、持续查询以及相关的状态管理机制，是有效使用 Flink Table API & SQL 进行流处理的关键。
