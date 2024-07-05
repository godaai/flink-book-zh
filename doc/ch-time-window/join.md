(join)=
# 双流关联

:::{note}

本教程已出版为《Flink 原理与实践》，感兴趣的读者请在各大电商平台购买！

<a href="https://item.jd.com/13154364.html"> ![](https://img.shields.io/badge/JD-%E8%B4%AD%E4%B9%B0%E9%93%BE%E6%8E%A5-red) </a>


:::

批处理经常要解决的问题是将两个数据源做关联，或者称为 Join。例如，很多手机 APP 都有一个用户数据源 User，同时 APP 会记录用户的行为，我们称之为 Behavior，两个表按照 userId 来进行 Join，如 {numref}`fig-join-example` 所示。Flink 支持流处理上的 Join，只不过 Flink 是在一个时间窗口上来进行两个流的 Join。

```{figure} ./img/join.png
---
name: fig-join-example
width: 80%
align: center
---
Join 示例图
```

目前，Flink 支持了两种 Join：Window Join（窗口连接）和 Interval Join（时间间隔连接）。

## Window Join

从名字中能猜到，Window Join 主要在 Flink 的窗口上进行操作，它将两个流中落在相同窗口的元素按照某个 Key 进行 Join。一个 Window Join 的大致骨架结构为：

```scala
input1.join(input2)
    .where(<KeySelector>)      <- input1 使用哪个字段作为 Key
    .equalTo(<KeySelector>)    <- input2 使用哪个字段作为 Key
    .window(<WindowAssigner>)  <- 指定 WindowAssigner
    [.trigger(<Trigger>)]      <- 指定 Trigger（可选）
    [.evictor(<Evictor>)]      <- 指定 Evictor（可选）
    .apply(<JoinFunction>)     <- 指定 JoinFunction
```

{numref}`fig-join-lifecycle` 展示了 Join 的大致过程。两个输入数据流先分别按 Key 进行分组，然后将元素划分到窗口中。窗口的划分需要使用 `WindowAssigner` 来定义，这里可以使用 Flink 提供的滚动窗口、滑动窗口或会话窗口等默认的 `WindowAssigner`。随后两个数据流中的元素会被分配到各个窗口上，也就是说一个窗口会包含来自两个数据流的元素。相同窗口内的数据会以内连接（Inner Join）的语义来相互关联，形成一个数据对。当窗口的时间结束，Flink 会调用 `JoinFunction` 来对窗口内的数据对进行处理。当然，我们也可以使用 `Trigger` 或 `Evictor` 做一些自定义优化，他们的使用方法和普通窗口的使用方法一样。

```{figure} ./img/join-lifecycle.png
---
name: fig-join-lifecycle
width: 80%
align: center
---
Join 的大致流程
```

接下来我们重点分析一下两个数据流是如何进行 Inner Join 的。

一般滴，Inner Join 只对两个数据源同时出现的元素做连接，形成一个数据对，即数据源 input1 中的某个元素与数据源 input2 中的所有元素逐个配对。当数据源某个窗口内没数据时，比如图中的第三个窗口，Join 的结果也是空的。

```{figure} ./img/tumbling-window-join.png
---
name: fig-tumbling-window-join
width: 80%
align: center
---
窗口内的数据 INNER JOIN
```

下面的代码自定义了 `JoinFunction`，并将 Join 结果打印出来。无论代码中演示的滚动窗口，还是滑动窗口或会话窗口，其原理都是一样的。

```java
public static class MyJoinFunction 
  implements JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String> {

    @Override
    public String join(Tuple2<String, Integer> input1, Tuple2<String, Integer> input2) {
        return "input 1 :" + input1.f1 + ", input 2 :" + input2.f1;
    }
}

DataStream<Tuple2<String, Integer>> input1 = ...
DataStream<Tuple2<String, Integer>> input2 = ...

DataStream<String> joinResult = input1.join(input2)
    .where(i1 -> i1.f0)
    .equalTo(i2 -> i2.f0)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
    .apply(new MyJoinFunction());
```

除了 `JoinFunction`，Flink 还提供了 `FlatJoinFunction`，其功能是输出零到多个结果。

如果 Inner Join 不能满足我们的需求，`CoGroupFunction` 提供了更多可自定义的功能，我们可以获得两个数据流中的所有元素，元素以 `Iterable<T>` 的形式供开发者使用。如果第一个数据流中的某些 Key 是空的，`CoGroupFunction` 被触发时，这个 Key 上的元素为空，开发者自己决定如何处理两个流里的数据。

```java
public static class MyCoGroupFunction 
  implements CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String> {
    @Override
    public void coGroup(Iterable<Tuple2<String, Integer>> input1, Iterable<Tuple2<String, Integer>> input2, Collector<String> out) {
        input1.forEach(element -> System.out.println("input1 :" + element.f1));
        input2.forEach(element -> System.out.println("input2 :" + element.f1));
    }
}
```

在主逻辑调用时，要写成 `input1.coGroup(input2).where(<KeySelector>).equalTo(<KeySelecotr>)`。

```java
DataStream<Tuple2<String, Integer>> input1 = ...
DataStream<Tuple2<String, Integer>> input2 = ...

DataStream<String> coGroupResult = input1.coGroup(input2)
      .where(i1 -> i1.f0)
      .equalTo(i2 -> i2.f0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .apply(new MyCoGroupFunction());
```

## Interval Join

与 Window Join 不同，Interval Join 不依赖 Flink 的 `WindowAssigner`，而是根据一个时间间隔（Interval）界定时间。Interval 需要一个时间下界（Lower Bound）和上界（Upper Bound），如果我们将 input1 和 input2 进行 Interval Join，input1 中的某个元素为 input1.element1，时间戳为 input1.element1.ts，那么一个 Interval 就是 [input1.element1.ts + lowerBound, input1.element1.ts + upperBound]，input2 中落在这个时间段内的元素将会和 input1.element1 组成一个数据对。用数学公式表达为，凡是符合下面公式的元素，会两两组合在一起。
$$
input1.element1.ts + lowerBound \le input2.elementX.ts \le input1.element1.ts + upperBound
$$
上下界可以是正数也可以是负数。

:::info
Flink（1.10）的 Interval Join 只支持 Event Time 语义。
:::

```{figure} ./img/interval-join.png
---
name: fig-interval-join
width: 80%
align: center
---
Interval Join 示例
```

下面的代码展示了如何对两个数据流进行 Interval Join：

```java
public static class MyProcessFunction extends ProcessJoinFunction<Tuple3<String, Long, Integer>, Tuple3<String, Long, Integer>, String> {
    @Override
    public void processElement(Tuple3<String, Long, Integer> input1,
                               Tuple3<String, Long, Integer> input2,
                               Context context,
                               Collector<String> out) {
      	out.collect("input 1: " + input1.toString() + ", input 2: " + input2.toString());
    }
}

// 使用 EventTime 时间语义
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
// 数据流有三个字段：（key, 时间戳, 数值）
DataStream<Tuple3<String, Long, Integer>> input1 = ...
DataStream<Tuple3<String, Long, Integer>> input2 = ...

DataStream<String> intervalJoinResult = input1.keyBy(i -> i.f0)
      .intervalJoin(input2.keyBy(i -> i.f0))
      .between(Time.milliseconds(-5), Time.milliseconds(10))
      .process(new MyProcessFunction());
```

默认的时间间隔是包含上下界的，我们可以使用 `.lowerBoundExclusive()` 和 `.upperBoundExclusive` 来确定是否需要包含上下界。

```java
DataStream<String> intervalJoinResult = input1.keyBy(i -> i.f0)
      .intervalJoin(input2.keyBy(i -> i.f0))
      .between(Time.milliseconds(-5), Time.milliseconds(10))
      .upperBoundExclusive()
      .lowerBoundExclusive()
      .process(new MyProcessFunction());
```

Interval Join 内部是用缓存来存储所有数据的，因此需要注意缓存数据不能太大，以免对内存造成太大压力。