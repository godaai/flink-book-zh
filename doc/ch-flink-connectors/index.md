# Flink 连接器

经过前文的学习，我们已经了解了 Flink 如何对一个数据流进行有状态的计算。在实际生产环境中，数据可能存放在不同的系统中，比如文件系统、数据库或消息队列。一个完整的 Flink 作业包括 Source 和 Sink 两大模块，Source 和 Sink 肩负着 Flink 与外部系统进行数据交互的重要功能，它们又被称为外部连接器（Connector）。本章将详细介绍 Flink 的 Connector 相关知识，主要内容如下。

```{tableofcontents}
```