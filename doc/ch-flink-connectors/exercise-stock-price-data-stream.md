(exercise-stock-price-data-stream)=
# 实验 读取并输出股票价格数据流

经过本章的学习，读者应该基本了解了Flink Connector的使用方法，本节我们继续以股票交易场景来模拟数据流的输入和输出。

## 一、实验目的

结合股票交易场景，学习如何使用Source和Sink，包括如何自定义Source、如何调用Kafka Sink。

## 二、实验内容

在第4章和第5章的实验中，我们都使用了股票交易数据，其中使用了StockPrice的数据结构，读取数据集中的数据来模拟一个真实数据流。这里我们将修改第4章实验中的Source，在读取数据集时使用一个Offset，保证Source有故障恢复的能力。

基于第5章中的对股票数据xVWAP的计算程序，使用Kafka Sink，将结果输出到Kafka。输出之前，需要在Kafka中建立对应的Topic。

## 三、实验要求

整个程序启用Flink的Checkpoint机制，计算xVWAP，需要重新编写Source，使其支持故障恢复，计算结果被发送到Kafka。计算结果可以使用JSON格式进行序列化。在命令行中启动一个Kafka Consumer来接收数据，验证程序输出的正确性。

## 四、实验报告

将思路和程序撰写成实验报告。

## 本章小结

通过本章的学习，读者应该可以了解Flink Connector的原理和使用方法，包括：端到端Exactly-Once的含义、自定义Source和Sink以及常用Flink Connector使用方法。相信通过本章的学习，读者已经可以将从Source到Sink的一整套流程串联起来。
