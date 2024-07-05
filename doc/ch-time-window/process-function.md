(process-function)=
# ProcessFunction

:::{note}

本教程已出版为《Flink 原理与实践》，感兴趣的读者请在各大电商平台购买！

<a href="https://item.jd.com/13154364.html"> ![](https://img.shields.io/badge/JD-%E8%B4%AD%E4%B9%B0%E9%93%BE%E6%8E%A5-red) </a>


:::

在继续介绍 Flink 时间和窗口相关操作之前，我们需要先了解一下 `ProcessFunction` 系列函数。它们是 Flink 体系中最底层的 API，提供了对数据流更细粒度的操作权限。之前提到的一些算子和函数能够进行一些时间上的操作，但是不能获取算子当前的 Processing Time 或者是 Watermark 时间戳，调用起来简单但功能相对受限。如果想获取数据流中 Watermark 的时间戳，或者使用定时器，需要使用 `ProcessFunction` 系列函数。Flink SQL 是基于这些函数实现的，一些需要高度个性化的业务场景也需要使用这些函数。

目前，这个系列函数主要包括 `KeyedProcessFunction`、`ProcessFunction`、`CoProcessFunction`、`KeyedCoProcessFunction`、`ProcessJoinFunction` 和 `ProcessWindowFunction` 等多种函数，这些函数各有侧重，但核心功能比较相似，主要包括两点：

* 状态：我们可以在这些函数中访问和更新 Keyed State 。

* 定时器（Timer）：像定闹钟一样设置定时器，我们可以在时间维度上设计更复杂的业务逻辑。

状态的介绍可以参考第六章的内容，本节将重点介绍 `ProcessFunction` 系列函数时间功能上的相关特性。

## Timer 的使用方法

说到时间相关的操作，就不能避开定时器（Timer）。我们可以把 Timer 理解成一个闹钟，使用前先在 Timer 中注册一个未来的时间，当这个时间到达，闹钟会“响起”，程序会执行一个回调函数，回调函数中执行一定的业务逻辑。这里以 `KeyedProcessFunction` 为例，来介绍 Timer 的注册和使用。

`ProcessFunction` 有两个重要的方法：`processElement()` 和 `onTimer()`，其中 `processElement` 函数在源码中的 Java 签名如下：


```java
// 处理数据流中的一条元素
public abstract void processElement(I value, Context ctx, Collector<O> out)
```

`processElement()` 方法处理数据流中的一条类型为 I 的元素，并通过 `Collector<O>` 输出出来。`Context` 是它区别于 `FlatMapFunction` 等普通函数的特色，开发者可以通过 `Context` 来获取时间戳，访问 `TimerService`，设置 Timer。

`ProcessFunction` 类中另外一个接口是 `onTimer()` 方法：

```java
// 时间到达后的回调函数
public void onTimer(long timestamp, OnTimerContext ctx, Collector<O> out)
```

这是一个回调函数，当到了“闹钟”时间，Flink 会调用 `onTimer()`，并执行一些业务逻辑。这里也有一个参数 `OnTimerContext`，它实际上是继承了上面的那个 `Context`，与 `Context` 几乎相同。

使用 Timer 的方法主要逻辑为：

1. 在 `processElement()` 方法中通过 `Context` 注册一个未来的时间戳 t。这个时间戳的语义可以是 Processing Time，也可以是 Event Time，根据业务需求来选择。
2. 在 `onTimer()` 方法中实现一些逻辑，到达 t 时刻，`onTimer()` 方法被自动调用。

从 `Context` 中，我们可以获取一个 `TimerService`，这是一个访问时间戳和 Timer 的接口。我们可以通过 `Context.timerService.registerProcessingTimeTimer()` 或 `Context.timerService.registerEventTimeTimer()` 这两个方法来注册 Timer，只需要传入一个时间戳即可。我们可以通过 `Context.timerService.deleteProcessingTimeTimer` 和 `Context.timerService.deleteEventTimeTimer` 来删除之前注册的 Timer。此外，还可以从中获取当前的时间戳：`Context.timerService.currentProcessingTime` 和 `Context.timerService.currentWatermark`。这些方法中，名字带有“ProcessingTime”的方法表示该方法基于 Processing Time 语义；名字带有“EventTime”或“Watermark”的方法表示该方法基于 Event Time 语义。

:::info
我们只能在 `KeyedStream` 上注册 Timer。每个 Key 下可以使用不同的时间戳注册不同的 Timer，但是每个 Key 的每个时间戳只能注册一个 Timer。如果想在一个 `DataStream` 上应用 Timer，可以将所有数据映射到一个伪造的 Key 上，但这样所有数据会流入一个算子子任务。
:::

我们再次以 [股票交易](../chapter-datastream-api/exercise-stock-basic.md) 场景来解释如何使用 Timer。一次股票交易包括：股票代号、时间戳、股票价格、成交量。我们现在想看一支股票未来是否一直连续上涨，如果一直上涨，则发送出一个提示。如果新数据比上次数据价格更高且目前没有注册 Timer，则注册一个未来的 Timer，如果在这期间价格降低则把刚才注册的 Timer 删除，如果在这期间价格没有降低，Timer 时间到达后触发 `onTimer()`，发送一个提示。下面的代码中，`intervalMills` 表示一个毫秒精度的时间段，如果这个时间段内一支股票价格一直上涨，则会输出文字提示。

```java
// 三个泛型分别为 Key、输入、输出
public static class IncreaseAlertFunction
    extends KeyedProcessFunction<String, StockPrice, String> {

    private long intervalMills;
    // 状态句柄
    private ValueState<Double> lastPrice;
    private ValueState<Long> currentTimer;

    public IncreaseAlertFunction(long intervalMills) throws Exception {
      	this.intervalMills = intervalMills;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 从 RuntimeContext 中获取状态
        lastPrice = getRuntimeContext().getState(
          new ValueStateDescriptor<Double>("lastPrice", Types.DOUBLE()));
        currentTimer = getRuntimeContext().getState(
          new ValueStateDescriptor<Long>("timer", Types.LONG()));
    }

    @Override
    public void processElement(StockPrice stock, Context context, Collector<String> out) throws Exception {

        // 状态第一次使用时，未做初始化，返回 null
        if (null == lastPrice.value()) {
          	// 第一次使用 lastPrice，不做任何处理
        } else {
            double prevPrice = lastPrice.value();
            long curTimerTimestamp;
            if (null == currentTimer.value()) {
              	curTimerTimestamp = 0;
            } else {
              	curTimerTimestamp = currentTimer.value();
            }
            if (stock.price < prevPrice) {
                // 如果新流入的股票价格降低，删除 Timer，否则该 Timer 一直保留
                context.timerService().deleteEventTimeTimer(curTimerTimestamp);
                currentTimer.clear();
            } else if (stock.price >= prevPrice && curTimerTimestamp == 0) {
                // 如果新流入的股票价格升高
                // curTimerTimestamp 为 0 表示 currentTimer 状态中是空的，还没有对应的 Timer
                // 新 Timer = 当前时间 + interval
                long timerTs = context.timestamp() + intervalMills;

                context.timerService().registerEventTimeTimer(timerTs);
                // 更新 currentTimer 状态，后续数据会读取 currentTimer，做相关判断
                currentTimer.update(timerTs);
            }
        }
        // 更新 lastPrice
        lastPrice.update(stock.price);
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) throws Exception {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        out.collect(formatter.format(ts) + ", symbol: " + ctx.getCurrentKey() +
                    " monotonically increased for " + intervalMills + " millisecond.");
        // 清空 currentTimer 状态
        currentTimer.clear();
    }
}
```

在主逻辑里，通过下面的 `process()` 算子调用 `KeyedProcessFunction`：

```java
DataStream<StockPrice> inputStream = ...

DataStream<String> warnings = inputStream
                .keyBy(stock -> stock.symbol)
                // 调用 process 函数
                .process(new IncreaseAlertFunction(3000));
```

Checkpoint 时，Timer 也会随其他状态数据一起保存起来。如果使用 Processing Time 语义设置一些 Timer，重启时这个时间戳已经过期，那些回调函数会立刻被调用执行。

## 侧输出

`ProcessFunction` 的另一大特色功能是可以将一部分数据发送到另外一个流中，而且输出到的两个流数据类型可以不一样。这个功能被称为为侧输出（Side Output）。我们通过 `OutputTag<T>` 来标记另外一个数据流：

```java
OutputTag<StockPrice> highVolumeOutput = 
  new OutputTag<StockPrice>("high-volume-trade"){};
```

在 `ProcessFunction` 中，我们可以使用 `Context.output` 方法将某类数据过滤出来。`OutputTag` 是这个方法的第一个参数，用来表示输出到哪个数据流。

```java
public static class SideOutputFunction 
  extends KeyedProcessFunction<String, StockPrice, String> {
    @Override
    public void processElement(StockPrice stock, Context context, Collector<String> out) throws Exception {
        if (stock.volume > 100) {
          	context.output(highVolumeOutput, stock);
        } else {
          	out.collect("normal tick data");
        }
    }
}
```

在主逻辑中，通过下面的方法先调用 `ProcessFunction`，再获取侧输出：

```java
DataStream<StockPrice> inputStream = ...

SingleOutputStreamOperator<String> mainStream = inputStream
    .keyBy(stock -> stock.symbol)
    // 调用 process 函数，包含侧输出逻辑
    .process(new SideOutputFunction());

DataStream<StockPrice> sideOutputStream = mainStream.getSideOutput(highVolumeOutput);
```

其中，`SingleOutputStreamOperator` 是 `DataStream` 的一种，它只有一种输出。下面是它在 Flink 源码中的定义：
```java
public class SingleOutputStreamOperator<T> extends DataStream<T> {
  	...
}
```

这个例子中，`KeyedProcessFunction` 的输出类型是 `String`，而 SideOutput 的输出类型是 `StockPrice`，两者可以不同。

## 在两个流上使用 `ProcessFunction` {#process-on-two-streams}

我们在 DataStream API 部分曾提到使用 `connect()` 将两个数据流的合并，如果想从更细的粒度在两个数据流进行一些操作，可以使用 `CoProcessFunction` 或 `KeyedCoProcessFunction`。这两个函数都有 `processElement1()` 和 `processElement2()` 方法，分别对第一个数据流和第二个数据流的每个元素进行处理。第一个数据流类型、第二个数据流类型和经过函数处理后的输出类型可以互不相同。尽管数据来自两个不同的流，但是他们可以共享同样的状态，所以可以参考下面的逻辑来实现两个数据流上的 Join：

* 创建一到多个状态，两个数据流都能访问到这些状态，这里以状态 a 为例。
* `processElement1()` 方法处理第一个数据流，更新状态 a。
* `processElement2()` 方法处理第二个数据流，根据状态 a 中的数据，生成相应的输出。

我们这次将股票价格结合媒体评价两个数据流一起讨论，假设对于某支股票有一个媒体评价数据流，媒体评价数据流包含了对该支股票的正负评价。两支数据流一起流入 `KeyedCoProcessFunction`，`processElement2()` 方法处理流入的媒体数据，将媒体评价更新到状态 `mediaState` 上，`processElement1()` 方法处理流入的股票交易数据，获取 `mediaState` 状态，生成到新的数据流。两个方法分别处理两个数据流，共享一个状态，通过状态来通信。

在主逻辑中，我们将两个数据流 `connect()`，然后按照股票代号进行 `keyBy()`，进而使用 `process()`：

```scala
// 读入股票数据流
DataStream<StockPrice> stockStream = ...

// 读入媒体评价数据流
DataStream<Media> mediaStream = ...

DataStream<StockPrice> joinStream = stockStream.connect(mediaStream)
    .keyBy("symbol", "symbol")
    // 调用 process 函数
    .process(new JoinStockMediaProcessFunction());
```

`KeyedCoProcessFunction` 的具体实现：

```scala
/**
  * 四个泛型：Key，第一个流类型，第二个流类型，输出。
  */
public static class JoinStockMediaProcessFunction extends KeyedCoProcessFunction<String, StockPrice, Media, StockPrice> {
    // mediaState
    private ValueState<String> mediaState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 从 RuntimeContext 中获取状态
        mediaState = getRuntimeContext().getState(
          	new ValueStateDescriptor<String>("mediaStatusState", Types.STRING));
    }

    @Override
    public void processElement1(StockPrice stock, Context context, Collector<StockPrice> collector) throws Exception {
        String mediaStatus = mediaState.value();
        if (null != mediaStatus) {
            stock.mediaStatus = mediaStatus;
            collector.collect(stock);
        }
    }

    @Override
    public void processElement2(Media media, Context context, Collector<StockPrice> collector) throws Exception {
        // 第二个流更新 mediaState
        mediaState.update(media.status);
    }
}
```

这个例子比较简单，没有使用 Timer，实际的业务场景中状态一般用到 Timer 将过期的状态清除。两个数据流的中间数据放在状态中，为避免状态的无限增长，需要使用 Timer 清除过期数据。

很多互联网 APP 的机器学习样本拼接都可能依赖这个函数来实现：服务端的机器学习特征是实时生成的，用户在 APP 上的行为是交互后产生的，两者属于两个不同的数据流，用户行为是机器学习所需要标注的正负样本，因此可以按照这个逻辑来将两个数据流拼接起来，通过拼接更快得到下一轮机器学习的样本数据。

:::info
使用 Event Time 时，两个数据流必须都设置好 Watermark，只设置一个流的 Event Time 和 Watermark，无法在 `CoProcessFunction` 和 `KeyedCoProcessFunction` 中使用 Timer 功能，因为 `process` 算子无法确定自己应该以怎样的时间来处理数据。
:::