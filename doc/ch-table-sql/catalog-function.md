(catalog-function)=
# 用户自定义函数

:::{note}

本教程已出版为《Flink 原理与实践》，感兴趣的读者请在各大电商平台购买！

<a href="https://item.jd.com/13154364.html"> ![](https://img.shields.io/badge/JD-%E8%B4%AD%E4%B9%B0%E9%93%BE%E6%8E%A5-red) </a>


:::

虽然 Flink 提供了丰富的[内置函数（System Functions）](system-function.md)来满足常见的计算需求，例如数学运算、字符串处理和聚合等，但在许多特定领域或业务场景下，这些内置函数可能并不足够。为了应对这些情况，Flink 允许开发者使用 Java、Scala 或 Python 编写自己的**用户自定义函数（User-Defined Functions, UDFs）**。

这些自定义函数极大地扩展了 Flink SQL 和 Table API 的处理能力，让你可以轻松实现特定的业务逻辑。由于自定义函数需要在使用前注册到 Flink 的 Catalog 中，它们有时也被称为 **Catalog Function**。

在本节中，我们将深入探讨如何创建和使用不同类型的用户自定义函数。

## 函数的注册与使用

无论创建哪种类型的 UDF，基本流程都类似：

1.  **定义函数**：创建一个类，继承 Flink 提供的特定函数基类（例如 `ScalarFunction`, `TableFunction`, `AggregateFunction`），并实现其核心逻辑方法（通常是 `eval` 方法）。
2.  **注册函数**：在 `TableEnvironment` 中，使用 `registerFunction` 方法将你的自定义函数实例注册到 Catalog 中，并为其指定一个唯一的名称。
3.  **调用函数**：在 Table API 或 SQL 查询中，通过注册时指定的名称来调用你的自定义函数。

下面是 `TableEnvironment` 中注册函数的基本接口示例（以 `ScalarFunction` 为例）：

```java
// 获取内部的 FunctionCatalog
FunctionCatalog functionCatalog = tableEnv.getFunctionCatalog();

/**
  * 注册一个 ScalarFunction
  * @param name 函数在 SQL/Table API 中使用的名称
  * @param function 自定义的 ScalarFunction 实例
  */
public void registerFunction(String name, ScalarFunction function) {
    // 实际上是将函数注册到 FunctionCatalog 中
    functionCatalog.registerTempSystemScalarFunction(name, function);
}
```

接下来，我们将分别详细介绍三种主要的 UDF 类型：标量函数、表函数和聚合函数。

## 标量函数（Scalar Function）

**标量函数**是最常见的 UDF 类型。它接收零个、一个或多个输入值，并返回**一个单独的标量值**。这与大多数编程语言中的标准函数非常相似。

**应用场景**：当你需要对一行数据中的一个或多个字段进行计算，并得出一个单一结果时，标量函数是理想的选择。例如，格式化字符串、进行数学计算、或根据某些条件返回布尔值等。

**示例：判断地理坐标是否在指定区域内**

假设我们正在处理包含经纬度信息的数据流，例如打车软件的订单数据或运动轨迹记录。我们想判断某个坐标点是否位于北京四环以内。由于 Flink 内置函数没有直接提供这种地理围栏功能，我们需要自定义一个标量函数来实现。

**1. 定义函数 `IsInFourRing`**

我们需要创建一个 Java 类 `IsInFourRing`，继承 `org.apache.flink.table.functions.ScalarFunction`，并实现 `eval` 方法来执行判断逻辑。

```java
import org.apache.flink.table.functions.ScalarFunction;

public class IsInFourRing extends ScalarFunction {

    // 定义北京四环的大致经纬度边界
    private static final double LON_EAST = 116.48;
    private static final double LON_WEST = 116.27;
    private static final double LAT_NORTH = 39.988;
    private static final double LAT_SOUTH = 39.83;

    // 核心逻辑：判断输入的经纬度是否在四环内
    // eval 方法的名称是固定的，Flink 会自动调用它
    // 参数类型和返回值类型定义了函数的签名
    public boolean eval(double lon, double lat) {
        return !(lon > LON_EAST || lon < LON_WEST) &&
               !(lat > LAT_NORTH || lat < LAT_SOUTH);
    }
}
```

在这个例子中，`eval` 方法接收两个 `double` 类型的参数（经度和纬度），并返回一个 `boolean` 类型的结果。

**2. 注册函数**

在使用这个函数之前，我们需要在 `TableEnvironment` 中注册它：

```java
// 假设 tEnv 是一个 TableEnvironment 实例
tEnv.registerFunction("IsInFourRing", new IsInFourRing());
```

这里我们将 `IsInFourRing` 类的一个实例注册到 Catalog 中，并指定其在 SQL 中使用的名称为 `IsInFourRing`。

**3. 在 SQL 中调用函数**

现在，我们可以在 SQL 查询的 `WHERE` 子句或其他允许标量函数的地方调用它了：

```java
// 假设 geoTable 是一个包含经纬度字段（long, lat）的表
Table geoTable = ...; 
tEnv.createTemporaryView("geo", geoTable);

// 使用自定义函数进行查询
Table inFourRingTab = tEnv.sqlQuery("SELECT id FROM geo WHERE IsInFourRing(long, lat)");
```

**函数重载**

与 Java/Scala 中的方法一样，你也可以为同一个函数名定义多个 `eval` 方法，只要它们的参数类型不同即可。Flink 会根据调用时传入的参数类型自动选择合适的 `eval` 方法。例如，我们可以为 `IsInFourRing` 添加处理 `float` 或 `String` 类型输入的方法：

```java
public class IsInFourRing extends ScalarFunction {
    // ... 四环边界定义 ...

    // 处理 double 类型
    public boolean eval(double lon, double lat) { ... }

    // 处理 float 类型
    public boolean eval(float lon, float lat) {
        // 可以将 float 转为 double 再调用上面的方法，或者直接比较
        return eval((double)lon, (double)lat);
    }

    // 处理 String 类型
    public boolean eval(String lonStr, String latStr) {
        try {
            double lon = Double.parseDouble(lonStr);
            double lat = Double.parseDouble(latStr);
            return eval(lon, lat);
        } catch (NumberFormatException e) {
            // 处理无效的字符串输入，例如返回 false
            return false;
        }
    }
}
```

**类型推断与提示 (`DataTypeHint`)**

通常，Flink 的类型系统能够自动推断出 `eval` 方法的输入和输出类型。但在某些情况下，特别是涉及到泛型或复杂类型时，自动推断可能会失败或不够精确。这时，你可以使用 `@DataTypeHint` 注解来明确指定类型。

例如，下面的函数计算两个时间戳之间的毫秒差，并提示 Flink 返回结果应为 `BIGINT` 类型：

```java
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import java.sql.Timestamp;

public class TimeDiff extends ScalarFunction {

    // 使用 @DataTypeHint 指定输出类型为 BIGINT
    @DataTypeHint("BIGINT")
    public Long eval(Timestamp t1, Timestamp t2) {
        if (t1 == null || t2 == null) {
            return null;
        }
        return t1.getTime() - t2.getTime(); // 返回毫秒差
    }
}
```

如果 `@DataTypeHint` 仍然无法满足需求，你可以重写 `UserDefinedFunction` 基类中的 `getTypeInference(DataTypeFactory)` 方法，实现更复杂的自定义类型推断逻辑。

## 表函数（Table Function）

**表函数**与标量函数的主要区别在于其输出：标量函数输出单个值，而表函数可以输出**零行、一行或多行数据**，每行可以包含一列或多列。

**应用场景**：当你需要根据输入参数生成一个“表”时，表函数非常有用。最典型的例子是字符串拆分（如按分隔符拆分一个字段生成多行）或解析 JSON/XML 结构。在 [SQL Join](sql-join.md) 中介绍的 Temporal Table Function 也是表函数的一种特殊形式。

**定义与使用**

定义表函数需要继承 `org.apache.flink.table.functions.TableFunction<T>`，其中泛型 `T` 定义了输出行的类型。核心逻辑同样在 `eval` 方法中实现，但你需要使用 `collect(T output)` 方法来输出每一行结果。

**示例：按分隔符拆分字符串**

假设我们有一个包含用户信息的字段，格式为 `Name#Age`，我们想将其拆分成两行：一行是名字，一行是年龄（虽然这个例子有点刻意，但能说明问题）。

**1. 定义函数 `SplitSharp`**

```java
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row; // 通常输出多列时使用 Row
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;

// @FunctionHint 提供了关于输出类型的元信息
@FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
public class SplitSharp extends TableFunction<Row> {

    public void eval(String str) {
        if (str == null) {
            return; // 不输出任何行
        }
        String[] parts = str.split("#");
        for (String part : parts) {
            // 使用 collect 输出一行，包含两个字段：单词和长度
            collect(Row.of(part, part.length()));
        }
    }
}
```

在这个例子中，我们输出了 `Row` 类型，每行包含一个 `String` 和一个 `Integer`。我们还使用了 `@FunctionHint` 和 `@DataTypeHint` 来明确指定输出的结构。

**2. 注册函数**

```java
tEnv.registerFunction("SplitSharpFunc", new SplitSharp());
```

**3. 在 SQL 中调用函数**

表函数通常出现在 SQL 的 `FROM` 子句中，并与主表进行 `JOIN`。你需要使用 `LATERAL TABLE(<FunctionName>(input_column))` 语法来调用它。

```java
// 假设 input_table 有一个字段 str，格式为 'Name#Age'
Table input_table = ...;
tEnv.createTemporaryView("input_table", input_table);

// 调用表函数，并将其输出命名为 T，包含 word 和 length 两个字段
// 使用逗号表示 CROSS JOIN
Table result = tEnv.sqlQuery(
    "SELECT id, word, length " +
    "FROM input_table, LATERAL TABLE(SplitSharpFunc(str)) AS T(word, length)"
);

// 也可以使用 LEFT JOIN，这样即使 str 无法拆分（或为 null），原始表的行也会保留
Table leftJoinResult = tEnv.sqlQuery(
    "SELECT id, T.word, T.length " +
    "FROM input_table LEFT JOIN LATERAL TABLE(SplitSharpFunc(str)) AS T(word, length) ON TRUE"
);
```

`LATERAL` 关键字是必需的，它表示表函数会针对 `input_table` 的**每一行**进行调用，并将输入行的列（如 `id`）与表函数产生的输出行（`word`, `length`）结合起来。
`AS T(word, length)` 为表函数产生的输出表及其列指定了别名。

`ON TRUE` 用于 `LEFT JOIN`，表示左表的每一行都应该尝试与右侧（表函数输出）进行连接。

## 聚合函数（Aggregate Function）

**聚合函数（Aggregate Function, AggFunction）** 用于将一个表（或一个分组）中的多行数据聚合成一个标量值。常见的内置聚合函数有 `COUNT`, `SUM`, `AVG`, `MAX`, `MIN` 等。

**应用场景**：当你需要对一组数据进行汇总计算时，例如计算总和、平均值、最大/最小值，或者实现更复杂的自定义聚合逻辑（如加权平均、计算中位数等）。

**定义与使用**

自定义聚合函数需要继承 `org.apache.flink.table.functions.AggregateFunction<T, ACC>`。这里有两个泛型参数：
*   `T`: 最终聚合结果的类型。
*   `ACC`: 累加器（Accumulator）的类型。累加器是在聚合过程中用于存储中间状态的对象。

你需要实现以下核心方法：

*   `createAccumulator()`: 创建并初始化一个累加器实例。对于每个聚合分组，Flink 会调用一次此方法。
*   `accumulate(ACC accumulator, [input_values...])`: 核心的累加逻辑。每当有一行新的输入数据到达时，Flink 会调用此方法，传入当前的累加器和输入值。你需要在此方法中更新累加器的状态。
*   `getValue(ACC accumulator)`: 在所有输入行都处理完毕后，Flink 调用此方法，传入最终的累加器状态，并要求返回最终的聚合结果（类型为 `T`）。

此外，根据具体的应用场景（例如会话窗口聚合、Retraction 场景），可能还需要实现其他方法：

*   `retract(ACC accumulator, [input_values...])`: （可选）用于处理数据的撤回（retraction）。当需要从累加器中移除之前某个输入值的影响时调用。
*   `merge(ACC accumulator, Iterable<ACC> its)`: （可选）用于合并多个累加器。在某些场景下（如会料窗口或批处理的部分聚合），Flink 可能需要将分属于同一分组但分布在不同累加器中的状态合并起来。
*   `resetAccumulator(ACC accumulator)`: （可选）重置累加器状态。

**示例：计算加权平均值**

假设我们要计算一组数值的加权平均值，其中每行数据包含一个值（`value`）和一个权重（`weight`）。加权平均值的计算公式是 `Sum(value * weight) / Sum(weight)`。

**1. 定义累加器 `WeightedAvgAccumulator`**

我们需要一个累加器来存储 `Sum(value * weight)` 和 `Sum(weight)`。

```java
/**
 * 累加器，用于存储加权平均计算的中间状态：
 * - sum: value * weight 的总和
 * - count: weight 的总和
 */
public class WeightedAvgAccumulator {
    public long sum = 0;
    public int count = 0;
}
```

**2. 定义聚合函数 `WeightedAvg`**

```java
import org.apache.flink.table.functions.AggregateFunction;

/**
 * 自定义聚合函数，计算加权平均值。
 * 输入: (BIGINT value, INT weight)
 * 输出: DOUBLE
 * 累加器: WeightedAvgAccumulator
 */
public class WeightedAvg extends AggregateFunction<Double, WeightedAvgAccumulator> {

    @Override
    public WeightedAvgAccumulator createAccumulator() {
        // 初始化累加器
        return new WeightedAvgAccumulator();
    }

    // 核心累加逻辑
    public void accumulate(WeightedAvgAccumulator acc, long value, int weight) {
        acc.sum += value * weight;
        acc.count += weight;
    }

    // 可选：处理数据撤回（例如在更新场景下）
    public void retract(WeightedAvgAccumulator acc, long value, int weight) {
        acc.sum -= value * weight;
        acc.count -= weight;
    }

    @Override
    public Double getValue(WeightedAvgAccumulator acc) {
        // 计算最终结果
        if (acc.count == 0) {
            return null; // 或者返回 0.0，取决于业务需求
        } else {
            return (double) acc.sum / acc.count;
        }
    }

    // 可选：合并多个累加器
    public void merge(WeightedAvgAccumulator acc, Iterable<WeightedAvgAccumulator> it) {
        for (WeightedAvgAccumulator otherAcc : it) {
            acc.count += otherAcc.count;
            acc.sum += otherAcc.sum;
        }
    }

    // 可选：重置累加器（通常在批处理中使用）
    public void resetAccumulator(WeightedAvgAccumulator acc) {
        acc.count = 0;
        acc.sum = 0L;
    }
}
```

**3. 注册函数**

```java
tEnv.registerFunction("WeightedAvgFunc", new WeightedAvg());
```

**4. 在 SQL 中调用函数**

聚合函数通常与 `GROUP BY` 子句一起使用。

```java
// 假设 input_table 包含 id, value, weight 字段
Table input_table = ...;
tEnv.createTemporaryView("input_table", input_table);

// 按 id 分组，计算每个 id 的加权平均值
Table result = tEnv.sqlQuery(
    "SELECT id, WeightedAvgFunc(value, weight) as wAvg " +
    "FROM input_table GROUP BY id"
);
```

## 表聚合函数（Table Aggregate Function）

**表聚合函数（Table Aggregate Function, TableAggFunction）** 与普通聚合函数类似，也是将多行输入聚合成结果，但它的输出不是单个标量值，而是**零行、一行或多行**，每行可以包含一列或多列。可以将其看作是聚合函数和表函数的结合。

**应用场景**：当你需要根据分组聚合的结果生成多行输出时，例如计算 Top N、根据聚合结果展开等。

**定义与使用**

定义表聚合函数需要继承 `org.apache.flink.table.functions.TableAggregateFunction<T, ACC>`，其中 `T` 是输出行的类型，`ACC` 是累加器类型。核心方法与普通聚合函数类似，但增加了一个 `emitValue(ACC accumulator, Collector<T> out)` 方法。

*   `createAccumulator()`: 创建累加器。
*   `accumulate(ACC accumulator, [input_values...])`: 累加逻辑。
*   `emitValue(ACC accumulator, Collector<T> out)`: 在所有输入处理完毕后调用，用于**计算并输出最终结果**。你需要使用 `out.collect(T resultRow)` 来输出一行或多行结果。

**示例：计算 Top 2**

假设我们要找出每个分组中值（`value`）最大的前两个。

**1. 定义累加器 `Top2Accumulator`**

累加器需要存储当前遇到的最大和第二大的值。

```java
import java.util.Objects;

/**
 * 累加器，存储 Top 2 的值。
 */
public class Top2Accumulator {
    public Integer first = null;
    public Integer second = null;
}
```

**2. 定义表聚合函数 `Top2`**

```java
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.types.Row;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;

// 指定输出类型为 ROW<value INT, rank INT>
@FunctionHint(output = @DataTypeHint("ROW<value INT, rank INT>"))
public class Top2 extends TableAggregateFunction<Row, Top2Accumulator> {

    @Override
    public Top2Accumulator createAccumulator() {
        return new Top2Accumulator();
    }

    // 累加逻辑：更新 Top 2
    public void accumulate(Top2Accumulator acc, Integer value) {
        if (value != null) {
            if (acc.first == null || value > acc.first) {
                acc.second = acc.first;
                acc.first = value;
            } else if (acc.second == null || value > acc.second) {
                acc.second = value;
            }
        }
    }

    // 可选：合并累加器（如果需要支持会话窗口等场景）
    public void merge(Top2Accumulator acc, Iterable<Top2Accumulator> it) {
        for (Top2Accumulator otherAcc : it) {
            if (otherAcc.first != null) {
                accumulate(acc, otherAcc.first);
            }
            if (otherAcc.second != null) {
                accumulate(acc, otherAcc.second);
            }
        }
    }

    // 输出最终结果：输出 Top 1 和 Top 2（如果存在）
    public void emitValue(Top2Accumulator acc, Collector<Row> out) {
        if (acc.first != null) {
            out.collect(Row.of(acc.first, 1)); // 输出 Top 1，rank 为 1
        }
        if (acc.second != null) {
            out.collect(Row.of(acc.second, 2)); // 输出 Top 2，rank 为 2
        }
    }
}
```

**3. 注册函数**

```java
tEnv.registerFunction("Top2Func", new Top2());
```

**4. 在 SQL 中调用函数**

表聚合函数需要和 `GROUP BY` 一起使用，并通过 `FLATMAP` Table Function 的方式在 `SELECT` 子句中展开结果。

```java
// 假设 input_table 包含 category 和 value 字段
Table input_table = ...;
tEnv.createTemporaryView("input_table", input_table);

// 按 category 分组，计算每个 category 的 Top 2 value
Table result = tEnv.sqlQuery(
    "SELECT category, value, rank " +
    "FROM input_table " +
    "GROUP BY category " +
    "FLATMAP(Top2Func(value)) AS (value, rank)"
);
```

这里 `FLATMAP(Top2Func(value))` 会对每个 `category` 分组调用 `Top2Func`，然后将其输出的多行（每个 Top 值一行）展开，并与分组键 `category` 组合成最终结果。

通过这四种类型的用户自定义函数（标量函数、表函数、聚合函数、表聚合函数），你可以极大地增强 Flink Table API & SQL 的处理能力，实现各种复杂的业务逻辑。