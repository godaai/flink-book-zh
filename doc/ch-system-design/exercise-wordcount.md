(exercise-wordcount)=
# 案例实战：WordCount

:::{note}

本教程已出版为《Flink 原理与实践》，感兴趣的读者请在各大电商平台购买！

<a href="https://item.jd.com/13154364.html"> ![](https://img.shields.io/badge/JD-%E8%B4%AD%E4%B9%B0%E9%93%BE%E6%8E%A5-red) </a>


:::

## 实验目的

熟悉 Flink 开发环境，尝试对 Flink WordCount 程序进行简单的修改。

## 实验内容

对 Flink WordCount 程序做一些修改。

## 实验要求

2.4 章节展示了如何使用 Flink 的 WordCount 样例程序，如何调试和提交作业。在这基础上，你可以开始尝试：

1. 修改 WordCount 程序：2.4 章节中的样例程序将输入按照空格隔开，真实世界中的文本通常包含各类标点符号，请修改代码，使你的程序不仅可以切分空格，也可以切分包括逗号、句号、冒号在内的标点符号（非单词字符），并统计词频。提示：可以使用正则表达式来判断哪些为非单词字符。

2. 使用 Flink 命令行工具提交该作业，并在集群监控看板上查看作业运行状态，Task Slot 的数量。

 你可以使用《哈姆雷特》中的经典台词作为输入数据：

To be, or not to be: that is the question:

Whether it’s nobler in the mind to suffer

The slings and arrows of outrageous fortune,

Or to take arms against a sea of troubles,

:::{tip}
我们可以在 IntelliJ Idea 的本地环境上进行本实验，无需启动一个 Flink 集群，执行环境的配置如下所示，打开相应链接也可以查看 Flink Web UI。如果输入依赖了 Kafka，需要提前启动 Kafka 集群，并向对应 Topic 里填充数据。
:::

```java
Configuration conf = new Configuration();
// 访问 http://localhost:8082 可以看到 Flink Web UI
conf.setInteger(RestOptions.PORT, 8082);
// 创建本地执行环境，并行度为 2
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2, conf);
```

## 实验报告

将思路和代码整理成实验报告。实验报告中包括你的代码，不同输入数据集时的输出内容，Flink 集群看板的运行截屏。