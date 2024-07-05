(user-define-functions)=
# 用户自定义函数

:::{note}

本教程已出版为《Flink 原理与实践》，感兴趣的读者请在各大电商平台购买！

<a href="https://item.jd.com/13154364.html"> ![](https://img.shields.io/badge/JD-%E8%B4%AD%E4%B9%B0%E9%93%BE%E6%8E%A5-red) </a>


:::

我们在 [Transformations](transformations.md) 部分中介绍了常用的一些操作，可以发现，使用 Flink 的算子必须进行自定义，自定义时可以使用 Lambda 表达式，也可以继承并重写函数类。本节将从源码和案例两方面对用户自定义函数进行一个总结和梳理。

## 函数类


对于 `map`、`flatMap`、`reduce` 等方法，我们可以实现 `MapFunction`、`FlatMapFunction`、`ReduceFunction` 等 `interface` 接口。这些函数类签名中都有泛型参数，用来定义该函数的输入或输出的数据类型。我们要继承这些类，并重写里面的自定义函数。以 `flatMap` 对应的 `FlatMapFunction` 为例，它在源码中的定义为：

```scala
package org.apache.flink.api.common.functions;

@FunctionalInterface
public interface FlatMapFunction<T, O> extends Function, Serializable {
  	void flatMap(T value, Collector<O> out) throws Exception;
}
```

这是一个函数式接口类，它继承了 Flink 的 `Function` 函数式接口。我们在第二章中提到函数式接口，这正是只有一个抽象函数方法的接口类，其目的是为了方便应用 Java Lambda 表达式。此外，它还继承了 `Serializable`，以便进行序列化，这是因为这些函数在运行过程中要发送到各个实例上，发送前后要进行序列化和反序列化。需要注意的是，使用这些函数时，一定要保证函数内的所有内容都可以被序列化。如果有一些不能被序列化的内容，或者使用接下来介绍的 RichFunction 函数类，或者重写 Java 的序列化和反序列化方法。

进一步观察 `FlatMapFunction` 发现，这个函数类有两个泛型 T 和 O，T 是输入，O 是输出。在继承这个接口类时，要设置好对应的输入和输出数据类型，否则会报错。我们最终其实是要重写虚函数 `flatMap`，函数的两个参数也与输入输出的泛型类型对应。参数 `value` 是 `flatMap` 的输入，数据类型是 T，参数 `out` 是 `flatMap` 的输出，它是一个 `Collector`，从 `Collector` 命名可以看出它起着收集的作用，最终输出成一个数据流，我们需要将类型为 O 的数据写入 `out`。

下面的例子继承 `FlatMapFunction`，并实现 `flatMap`，只对长度大于 limit 的字符串切词：

```scala
// 使用 FlatMapFunction 实现过滤逻辑，只对字符串长度大于 limit 的内容进行词频统计
public static class WordSplitFlatMap implements FlatMapFunction<String, String> {

    private Integer limit;

    public WordSplitFlatMap(Integer limit) {
      	this.limit = limit;
    }

    @Override
    public void flatMap(String input, Collector<String> collector) throws Exception {
      	if (input.length() > limit) {
        		for (String word: input.split(" "))
        			collector.collect(word);
      }
    }
}

DataStream<String> dataStream = senv.fromElements("Hello World", "Hello this is Flink");
DataStream<String> functionStream = dataStream.flatMap(new WordSplitFlatMap(10));
```

## Lambda 表达式

当不需要处理非常复杂的业务逻辑时，使用 Lambda 表达式可能是更好的选择，Lambda 表达式能让代码更简洁紧凑。Java 和 Scala 都可以支持 Lambda 表达式。

### Scala 的 Lambda 表达式

我们先看对 Lambda 表达式支持最好的 Scala。对于 `flatMap`，Flink 的 Scala 源码有三种定义，我们先看一下第一种的定义：

```scala
def flatMap[O: TypeInformation](fun: (T, Collector[O]) => Unit): DataStream[O] = {...}
```

`flatMap` 输入是泛型 T，输出是泛型 O，接收一个名为 `fun` 的 Lambda 表达式，`fun` 形如 `(T, Collector[O] => {...})`。Lambda 表达式要将数据写到 `Collector[O]` 中。

我们继续以切词为例，程序可以写成下面的样子，`flatMap` 中的内容是一个 Lambda 表达式。其中的 `foreach(out.collect)` 本质上也是一个 Lambda 表达式。从这个例子可以看出，Scala 的无所不在的函数式编程思想。

```scala
val lambda = dataStream.flatMap{
  (value: String, out: Collector[String]) => {
    if (value.size > 10) {
      value.split(" ").foreach(out.collect)
    }
  }
}
```

然后我们看一下源码中 Scala 的第二种定义：

```scala
def flatMap[O: TypeInformation](fun: T => TraversableOnce[O]): DataStream[O] = {...}
```

与之前的不同，这里的 Lambda 表达式输入是泛型 T，输出是一个 `TraversableOnce[O]`,`TraversableOnce` 表示这是一个 O 组成的列表。与之前使用 `Collector` 收集输出不同，这里直接输出一个列表，Flink 帮我们将列表做了展平。使用 `TraversableOnce` 也导致我们无论如何都要返回一个列表，即使是一个空列表，否则无法匹配函数的定义。总结下来，这种场景的 Lambda 表达式输入是一个 T，无论如何输出都是一个 O 的列表，即使是一个空列表。

```scala
// 只对字符串数量大于 15 的句子进行处理
val longSentenceWords = dataStream.flatMap {
  input => {
    if (input.size > 15) {
      // 输出是 TraversableOnce 因此返回必须是一个列表
      // 这里将 Array[String] 转成了 Seq[String]
      input.split(" ").toSeq
    } else {
      // 为空时必须返回空列表，否则返回值无法与 TraversableOnce 匹配！
      Seq.empty
    }
  }
}
```

在使用 Lambda 表达式时，我们应该逐渐学会使用 IntelliJ Idea 的类型检查和匹配功能。比如在本例中，如果返回值不是一个 `TraversableOnce`，那么 IntelliJ Idea 会将该行标红，告知我们输入或输出的类型不匹配。

此外，还有第三种只针对 Scala 的 Lambda 表达式使用方法。Flink 为了保持 Java 和 Scala API 的一致性，一些 Scala 独有的特性没有被放入标准的 API，而是集成到了一个扩展包中。这种 API 支持类型匹配的偏函数（Partial Function），结合 case 关键字，能够在语义上更好地描述数据类型：

```scala
val data: DataStream[(String, Long, Double)] = ...
data.flatMapWith {
  case (symbol, timestamp, price) => // ...
}
```

使用这种 API 时，需要添加引用：

```scala
import org.apache.flink.streaming.api.scala.extensions._
```

这种方式给输入定义了变量名和类型，方便阅读者阅读代码，同时也保留了函数式编程的简洁。Spark 的大多数算子默认都支持此功能，Flink 没有默认支持此功能，而是将这个功能放到了扩展包里，对于 Spark 用户来说，迁移到 Flink 时需要注意这个区别。此外 `mapWith`、`filterWith`、`keyingBy`、`reduceWith` 也分别是其他算子相对应的接口。

使用 `flatMapWith`，之前的切词可以实现为：

```scala
val flatMapWith = dataStream.flatMapWith {
  case (sentence: String) => {
    if (sentence.size > 15) {
      sentence.split(" ").toSeq
    } else {
      Seq.empty
    }
  }
}
```

### Java

再来看看 Java，因为一些遗留问题，它的 Lambda 表达式使用起来有一些区别。

第二章中提到，Java 有类型擦除问题，`void flatMap(IN value, Collector<OUT> out)` 编译成了 `void flatMap(IN value, Collector out)`，擦除了泛型信息，Flink 无法自动获取返回类型，如果不做其他操作，会抛出异常。

```
org.apache.flink.api.common.functions.InvalidTypesException: The generic type parameters of 'Collector' are missing.
    In many cases lambda methods don't provide enough information for automatic type extraction when Java generics are involved.
    An easy workaround is to use an (anonymous) class instead that implements the 'org.apache.flink.api.common.functions.FlatMapFunction' interface.
    Otherwise the type has to be specified explicitly using type information.
```

这种情况下，根据报错提示，或者使用一个类实现 `FlatMapFunction`（包括匿名类），或者添加类型信息。这个类型信息，正是 [数据类型和序列化](./data-types) 章节中所介绍的数据类型。

```java
DataStream<String> words = dataStream.flatMap (
    (String input, Collector<String> collector) -> {
        for (String word : input.split(" ")) {
          	collector.collect(word);
        }
    })
  	// 提供类型信息以解决类型擦除问题
  	.returns(Types.STRING);
```

通过这里对 Scala 和 Java 的对比不难发现，Scala 更灵活，Java 更严谨，各有优势。

## Rich 函数类

在上面两种自定义方法的基础上，Flink 还提供了 RichFunction 函数类。从名称上来看，这种函数类在普通的函数类上增加了 Rich 前缀，比如 `RichMapFunction`、`RichFlatMapFunction` 或 `RichReduceFunction` 等等。比起普通的函数类，Rich 函数类增加了：

* `open()` 方法：Flink 在算子调用前会执行这个方法，可以用来进行一些初始化工作。
* `close()` 方法：Flink 在算子最后一次调用结束后执行这个方法，可以用来释放一些资源。
* `getRuntimeContext()` 方法：获取运行时上下文。每个并行的算子子任务都有一个运行时上下文，上下文记录了这个算子运行过程中的一些信息，包括算子当前的并行度、算子子任务序号、广播数据、累加器、监控数据。最重要的是，我们可以从上下文里获取状态数据。

我们可以看一下源码中的函数签名：

```java
public abstract class RichFlatMapFunction<IN, OUT> extends AbstractRichFunction implements FlatMapFunction<IN, OUT>
```

它既实现了 `FlatMapFunction` 接口类，又继承了 `AbstractRichFunction`。其中 `AbstractRichFunction` 是一个抽象类，有一个成员变量 `RuntimeContext`，有 `open`、`close` 和 `getRuntimeContext` 等方法。

我们尝试继承并实现 `RichFlatMapFunction`，并使用一个累加器。首先简单介绍累加器的概念：在单机环境下，我们可以用一个 for 循环做累加统计，但是在分布式计算环境下，计算是分布在多台节点上的，每个节点处理一部分数据，因此单纯循环无法满足计算，累加器是大数据框架帮我们实现的一种机制，允许我们在多节点上进行累加统计。

```java
// 实现 RichFlatMapFunction 类
// 添加了累加器 Accumulator
public static class WordSplitRichFlatMap extends RichFlatMapFunction<String, String> {

    private int limit;

    // 创建一个累加器
    private IntCounter numOfLines = new IntCounter(0);

    public WordSplitRichFlatMap(Integer limit) {
      	this.limit = limit;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 在 RuntimeContext 中注册累加器
        getRuntimeContext().addAccumulator("num-of-lines", this.numOfLines);
    }

    @Override
    public void flatMap(String input, Collector<String> collector) throws Exception {

        // 运行过程中调用累加器
        this.numOfLines.add(1);

        if(input.length() > limit) {
            for (String word: input.split(" "))
            collector.collect(word);
        }
    }
}
```

在主逻辑中获取作业执行的结果，得到累加器中的值。

```java
// 获取作业执行结果
JobExecutionResult jobExecutionResult = senv.execute("basic flatMap transformation");
// 执行结束后 获取累加器的结果
Integer lines = jobExecutionResult.getAccumulatorResult("num-of-lines");
System.out.println("num of lines: " + lines);
```

累加器是 RichFunction 函数类提供的众多功能之一，RichFunction 函数类最具特色的功能是第六章将要介绍的有状态计算。