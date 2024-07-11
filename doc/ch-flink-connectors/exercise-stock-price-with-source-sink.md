(sec-exercise-stock-price-with-source-sink)=
# 案例实战：读取并输出股票价格数据流

经过本章的学习，读者应该基本了解了 Flink Connector 的使用方法，本节我们继续以股票交易场景来模拟数据流的输入和输出。

## 一、实验目的

结合股票交易场景，学习如何使用 Source 和 Sink，包括如何自定义 Source、如何调用 Kafka Sink。

## 二、实验内容

在第 4 章和第 5 章的实验中，我们都使用了股票交易数据，其中使用了 StockPrice 的数据结构，读取数据集中的数据来模拟一个真实数据流。这里我们将修改第 4 章实验中的 Source，在读取数据集时使用一个 Offset，保证 Source 有故障恢复的能力。

基于第 5 章中的对股票数据 xVWAP 的计算程序，使用 Kafka Sink，将结果输出到 Kafka。输出之前，需要在 Kafka 中建立对应的 Topic。

## 三、实验要求

整个程序启用 Flink 的 Checkpoint 机制，计算 xVWAP，需要重新编写 Source，使其支持故障恢复，计算结果被发送到 Kafka。计算结果可以使用 JSON 格式进行序列化。在命令行中启动一个 Kafka Consumer 来接收数据，验证程序输出的正确性。

## 四、实验报告

将思路和程序撰写成实验报告。

## 本章小结

通过本章的学习，读者应该可以了解 Flink Connector 的原理和使用方法，包括：端到端 Exactly-Once 的含义、自定义 Source 和 Sink 以及常用 Flink Connector 使用方法。相信通过本章的学习，读者已经可以将从 Source 到 Sink 的一整套流程串联起来。
